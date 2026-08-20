#!/usr/bin/env bash
#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

# Re-vendors/re-forks the non-generated Kafka protocol support classes and their Apache Kafka unit
# tests into kroxylicious-api/src/{main,test}/java/io/kroxylicious/kafka/**.
#
# This is a REPRODUCIBLE process, not a one-off hand copy. Given a clean Apache Kafka checkout at
# the target tag it will regenerate the entire vendored/forked surface deterministically, for each
# of the two Maven source roots (main, test — we only ever pull from the one `clients` module, so
# there's no module-level split above this):
#
#   1. copy the frozen file list ($root/vendored-files.txt) from the Kafka checkout,
#   2. apply declarative content-based edits ($root/edits.yaml, via apply-edits.py) to cut the
#      server/config edges we do not vendor, or to trim a file to the handful of members our forked
#      tests actually need (see test/edits.yaml's preserveBlocks entries),
#   3. relocate org.apache.kafka.* -> io.kroxylicious.kafka.* via OpenRewrite (rewrite.yml), which
#      also moves files to the new package path,
#   4. sync the result into kroxylicious-api/src/$root/java,
#   5. run `mvn process-test-sources` so formatter-maven-plugin/impsort-maven-plugin reformat
#      brace style and import order to match every other module's committed source — without this
#      the tree is functionally correct but diffs large and spuriously against the committed form.
#      process-test-sources (not process-sources) is required: formatter-maven-plugin binds its
#      test-source formatting to the process-test-sources phase, which process-sources does not
#      reach, so src/test/java would otherwise come out unformatted.
#
# The generated *Data / *DataJsonConverter message classes are NOT handled here; they are produced
# from the pinned protocol JSON specs by the build (see the module pom + kafka.message-spec.version).
#
# Usage:  vendor.sh <path-to-apache-kafka-checkout>
# The checkout MUST be at the tag recorded in main/vendored-files.txt's header (see
# kafka.message-spec.version).
set -euo pipefail

KAFKA="${1:?usage: vendor.sh <path-to-apache-kafka-checkout>}"
HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
API_SRC="$ROOT/kroxylicious-api/src"

STAGE="$(mktemp -d)"
VENV="$(mktemp -d)"
trap 'rm -rf "$STAGE" "$VENV"' EXIT

python3 -m venv "$VENV"
"$VENV/bin/pip" install -q -r "$HERE/requirements.txt"

for root in main test; do
  SRC_ROOT="$KAFKA/clients/src/$root/java"
  [ -d "$SRC_ROOT" ] || { echo "ERROR: $SRC_ROOT not found — is '$KAFKA' an Apache Kafka checkout?" >&2; exit 1; }
  COPY_ROOT="$STAGE/src/$root/java"
  mkdir -p "$COPY_ROOT"

  echo "==> [$root] copying $(grep -c '^org/.*\.java$' "$HERE/$root/vendored-files.txt") files from $SRC_ROOT"
  while IFS= read -r rel; do
    # vendored-files.txt carries a license header and description before the file list; skip
    # anything that isn't a file entry. Every real entry is org/apache/kafka/**.java — checking
    # only a *.java suffix is not enough, since a wrapped comment line can end in a bare
    # "Foo.java" mention (a real incident, not hypothetical).
    [[ "$rel" == org/*.java ]] || continue
    mkdir -p "$COPY_ROOT/$(dirname "$rel")"
    cp "$SRC_ROOT/$rel" "$COPY_ROOT/$rel"
  done < "$HERE/$root/vendored-files.txt"

  echo "==> [$root] applying edits ($root/edits.yaml)"
  "$VENV/bin/python3" "$HERE/apply-edits.py" "$COPY_ROOT" "$HERE/$root/edits.yaml"
done

echo "==> relocating packages with OpenRewrite (org.apache.kafka -> io.kroxylicious.kafka)"
cp "$HERE/rewrite.yml" "$STAGE/rewrite.yml"
cp "$HERE/rewrite-pom.xml" "$STAGE/pom.xml"
# OpenRewrite honours .gitignore, so the staging dir must be its own git repo for the copied
# files to be visible to the recipe.
( cd "$STAGE" && git init -q && git add -A && git -c user.email=vendor@kroxylicious -c user.name=vendor commit -qm staged )
( cd "$STAGE" && mvn -q -B org.openrewrite.maven:rewrite-maven-plugin:6.46.1:run )

for root in main test; do
  REWRITTEN="$STAGE/src/$root/java/io/kroxylicious/kafka"
  [ -d "$REWRITTEN" ] || { echo "ERROR: OpenRewrite did not produce $REWRITTEN" >&2; exit 1; }

  DEST="$API_SRC/$root/java/io/kroxylicious/kafka"
  echo "==> [$root] syncing into $DEST"
  mkdir -p "$DEST"
  # Wipe only the vendored/forked (non-message) tree; the generated message.* package is never
  # committed here.
  find "$DEST" -mindepth 1 -type d -name message -prune -o -type f -name '*.java' -print | xargs -r rm -f
  rsync -a --exclude='message/' "$REWRITTEN/" "$DEST/"
done

echo "==> formatting (mvn -pl kroxylicious-api -am process-test-sources)"
( cd "$ROOT" && mvn -q -pl kroxylicious-api -am process-test-sources )

echo "==> done: $(find "$API_SRC/main/java/io/kroxylicious/kafka" -name '*.java' ! -path '*/message/*' | wc -l | tr -d ' ') main files, $(find "$API_SRC/test/java/io/kroxylicious/kafka" -name '*.java' | wc -l | tr -d ' ') test files"
