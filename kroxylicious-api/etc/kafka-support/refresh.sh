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
#   1. copy the frozen file list (closure.txt) from the Kafka checkout,
#   2. apply content-based surgery (surgery.py) to cut the server/config edges we do not vendor,
#   3. relocate org.apache.kafka.common.* -> io.kroxylicious.kafka.common.* via OpenRewrite
#      (rewrite.yml), which also moves files to the new package path,
#   4. sync the result into src/main/java.
#
# The generated *Data / *DataJsonConverter message classes are NOT handled here; they are produced
# from the pinned protocol JSON specs by the build (see the module pom + kafka.message-spec.version).
#
# Usage:  refresh.sh <path-to-apache-kafka-checkout>
# The checkout MUST be at the tag recorded in closure.txt's header (see kafka.message-spec.version).
set -euo pipefail

KAFKA="${1:?usage: refresh.sh <path-to-apache-kafka-checkout>}"
HERE="$(cd "$(dirname "$0")" && pwd)"
API_SRC="$(cd "$HERE/../../src/main/java" && pwd)"
SRC_ROOT="$KAFKA/clients/src/main/java/org/apache/kafka/common"
[ -d "$SRC_ROOT" ] || { echo "ERROR: $SRC_ROOT not found — is '$KAFKA' an Apache Kafka checkout?" >&2; exit 1; }

# Surgery targets (the 4 files whose server/config edges we cut). Kept in sync with surgery.py.
SURGERY="record/internal/CompressionType.java utils/Utils.java record/internal/DefaultRecordBatch.java record/internal/AbstractLegacyRecordBatch.java"

STAGE="$(mktemp -d)"
trap 'rm -rf "$STAGE"' EXIT
COPY_ROOT="$STAGE/src/main/java/org/apache/kafka/common"
mkdir -p "$COPY_ROOT"


echo "==> copying $(grep -c . "$HERE/closure.txt") files from $SRC_ROOT"
while IFS= read -r rel; do
  [ -z "$rel" ] && continue
  mkdir -p "$COPY_ROOT/$(dirname "$rel")"
  cp "$SRC_ROOT/$rel" "$COPY_ROOT/$rel"
done < "$HERE/closure.txt"

echo "==> applying surgery"
for f in $SURGERY; do
  python3 "$HERE/surgery.py" "$COPY_ROOT/$f"
done

echo "==> relocating packages with OpenRewrite (org.apache.kafka.common -> io.kroxylicious.kafka.common)"
cp "$HERE/rewrite.yml" "$STAGE/rewrite.yml"
cat > "$STAGE/pom.xml" <<'POM'
<project xmlns="http://maven.apache.org/POM/4.0.0"><modelVersion>4.0.0</modelVersion>
  <groupId>io.kroxylicious.tools</groupId><artifactId>kafka-support-rewrite</artifactId>
  <version>1.0</version><packaging>jar</packaging>
  <properties><maven.compiler.release>21</maven.compiler.release>
    <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
    <!-- rewrite:run forks the lifecycle through 'compile'. The staged sources deliberately do not
         compile here (they still reference org.apache.kafka before relocation, and native codec libs
         are off this throwaway classpath). ChangePackage is textual and needs no attribution, so we
         let the compile attempt fail without aborting the build. -->
    <maven.compiler.failOnError>false</maven.compiler.failOnError></properties>
  <build><plugins><plugin>
    <groupId>org.openrewrite.maven</groupId><artifactId>rewrite-maven-plugin</artifactId>
    <version>6.46.1</version>
    <configuration><activeRecipes><recipe>io.kroxylicious.RewriteCopiedKafka</recipe></activeRecipes></configuration>
  </plugin></plugins></build>
</project>
POM
# OpenRewrite honours .gitignore, so the staging dir must be its own git repo for the copied
# files to be visible to the recipe.
( cd "$STAGE" && git init -q && git add -A && git -c user.email=refresh@kroxylicious -c user.name=refresh commit -qm staged )
( cd "$STAGE" && mvn -q -B org.openrewrite.maven:rewrite-maven-plugin:6.46.1:run )

REWRITTEN="$STAGE/src/main/java/io/kroxylicious/kafka/common"
[ -d "$REWRITTEN" ] || { echo "ERROR: OpenRewrite did not produce $REWRITTEN" >&2; exit 1; }

echo "==> syncing into $API_SRC/io/kroxylicious/kafka/common"
DEST="$API_SRC/io/kroxylicious/kafka/common"
mkdir -p "$DEST"
# Wipe only the vendored (non-message) tree; the generated message.* package is never committed here.
find "$DEST" -mindepth 1 -type d -name message -prune -o -type f -name '*.java' -print | xargs -r rm -f
rsync -a --exclude='message/' "$REWRITTEN/" "$DEST/"

echo "==> done: $(find "$DEST" -name '*.java' ! -path '*/message/*' | wc -l | tr -d ' ') support files vendored"
