#!/usr/bin/env bash
#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#
# Re-vendors the non-generated Kafka protocol support classes into
# kroxylicious-api/src/main/java/io/kroxylicious/kafka/common/**.
#
# This is a REPRODUCIBLE process, not a one-off hand copy. Given a clean Apache Kafka checkout at
# the target tag it will regenerate the entire vendored surface deterministically:
#
#   1. copy the frozen file list (vendored-files.txt) from the Kafka checkout,
#   2. apply declarative content-based edits (edits.yaml, via apply-edits.py) to cut the
#      server/config edges we do not vendor,
#   3. relocate org.apache.kafka.common.* -> io.kroxylicious.kafka.common.* via OpenRewrite
#      (rewrite.yml), which also moves files to the new package path,
#   4. sync the result into kroxylicious-api/src/main/java,
#   5. run `mvn process-sources` so formatter-maven-plugin/impsort-maven-plugin reformat brace
#      style and import order to match every other module's committed source — without this the
#      tree is functionally correct but diffs large and spuriously against the committed form.
#
# The generated *Data / *DataJsonConverter message classes are NOT handled here; they are produced
# from the pinned protocol JSON specs by the build (see the module pom + kafka.message-spec.version).
#
# Usage:  vendor.sh <path-to-apache-kafka-checkout>
# The checkout MUST be at the tag recorded in vendored-files.txt's header (see
# kafka.message-spec.version).
set -euo pipefail

KAFKA="${1:?usage: vendor.sh <path-to-apache-kafka-checkout>}"
HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
API_SRC="$ROOT/kroxylicious-api/src/main/java"
SRC_ROOT="$KAFKA/clients/src/main/java/org/apache/kafka/common"
[ -d "$SRC_ROOT" ] || { echo "ERROR: $SRC_ROOT not found — is '$KAFKA' an Apache Kafka checkout?" >&2; exit 1; }

STAGE="$(mktemp -d)"
VENV="$(mktemp -d)"
trap 'rm -rf "$STAGE" "$VENV"' EXIT
COPY_ROOT="$STAGE/src/main/java/org/apache/kafka/common"
mkdir -p "$COPY_ROOT"

echo "==> copying $(grep -vc '^#\|^$' "$HERE/vendored-files.txt") files from $SRC_ROOT"
while IFS= read -r rel; do
  [ -z "$rel" ] && continue
  [[ "$rel" == \#* ]] && continue
  mkdir -p "$COPY_ROOT/$(dirname "$rel")"
  cp "$SRC_ROOT/$rel" "$COPY_ROOT/$rel"
done < "$HERE/vendored-files.txt"

echo "==> applying edits (edits.yaml)"
python3 -m venv "$VENV"
"$VENV/bin/pip" install -q -r "$HERE/requirements.txt"
"$VENV/bin/python3" "$HERE/apply-edits.py" "$COPY_ROOT" "$HERE/edits.yaml"

echo "==> relocating packages with OpenRewrite (org.apache.kafka.common -> io.kroxylicious.kafka.common)"
cp "$HERE/rewrite.yml" "$STAGE/rewrite.yml"
cp "$HERE/rewrite-pom.xml" "$STAGE/pom.xml"
# OpenRewrite honours .gitignore, so the staging dir must be its own git repo for the copied
# files to be visible to the recipe.
( cd "$STAGE" && git init -q && git add -A && git -c user.email=vendor@kroxylicious -c user.name=vendor commit -qm staged )
( cd "$STAGE" && mvn -q -B org.openrewrite.maven:rewrite-maven-plugin:6.46.1:run )

REWRITTEN="$STAGE/src/main/java/io/kroxylicious/kafka/common"
[ -d "$REWRITTEN" ] || { echo "ERROR: OpenRewrite did not produce $REWRITTEN" >&2; exit 1; }

echo "==> syncing into $API_SRC/io/kroxylicious/kafka/common"
DEST="$API_SRC/io/kroxylicious/kafka/common"
mkdir -p "$DEST"
# Wipe only the vendored (non-message) tree; the generated message.* package is never committed here.
find "$DEST" -mindepth 1 -type d -name message -prune -o -type f -name '*.java' -print | xargs -r rm -f
rsync -a --exclude='message/' "$REWRITTEN/" "$DEST/"

echo "==> formatting (mvn -pl kroxylicious-api -am process-sources)"
( cd "$ROOT" && mvn -q -pl kroxylicious-api -am process-sources )

echo "==> done: $(find "$DEST" -name '*.java' ! -path '*/message/*' | wc -l | tr -d ' ') support files vendored"
