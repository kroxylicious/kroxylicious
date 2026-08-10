#!/usr/bin/env bash
#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

# Tests for validate-module-structure.sh
#
# Usage: validate-module-structure-test.sh

set -uo pipefail

SCRIPT="$(cd "$(dirname "$0")" && pwd)/validate-module-structure.sh"
PASS=0
FAIL=0

WORK_DIR=$(mktemp -d)
trap 'rm -rf "$WORK_DIR"' EXIT

# ---- Test helpers ----

assert_passes() {
    local desc="$1" pom="$2"
    if bash "$SCRIPT" "$pom" >/dev/null 2>&1; then
        echo "PASS: $desc"
        PASS=$((PASS + 1))
    else
        echo "FAIL: $desc (expected exit 0, got non-zero)"
        FAIL=$((FAIL + 1))
    fi
}

assert_fails() {
    local desc="$1" pom="$2"
    if ! bash "$SCRIPT" "$pom" >/dev/null 2>&1; then
        echo "PASS: $desc"
        PASS=$((PASS + 1))
    else
        echo "FAIL: $desc (expected non-zero exit, got 0)"
        FAIL=$((FAIL + 1))
    fi
}

# Write a minimal intermediate parent pom.xml into $dir with the given leaf modules.
make_parent_pom() {
    local dir="$1"; shift
    mkdir -p "$dir"
    {
        echo '<project xmlns="http://maven.apache.org/POM/4.0.0">'
        echo '  <modelVersion>4.0.0</modelVersion>'
        echo '  <groupId>test</groupId><artifactId>parent</artifactId><version>1</version>'
        echo '  <packaging>pom</packaging>'
        echo '  <modules>'
        for leaf in "$@"; do echo "    <module>$leaf</module>"; done
        echo '  </modules>'
        echo '</project>'
    } > "$dir/pom.xml"
}

# Write a root pom.xml with a given root-modules block and build-the-world module list.
make_root_pom() {
    local path="$1"
    local root_modules_block="$2"; shift 2   # e.g. '<modules/>' or '<modules><module>x</module></modules>'
    {
        echo '<project xmlns="http://maven.apache.org/POM/4.0.0">'
        echo '  <modelVersion>4.0.0</modelVersion>'
        echo '  <groupId>test</groupId><artifactId>root</artifactId><version>1</version>'
        echo "  $root_modules_block"
        echo '  <profiles>'
        echo '    <profile><id>build-the-world</id><modules>'
        for group in "$@"; do echo "      <module>$group</module>"; done
        echo '    </modules></profile>'
        echo '  </profiles>'
        echo '</project>'
    } > "$path"
}

# ---- Fixtures ----

# Valid structure: empty root modules, two non-empty intermediate parents
VALID="$WORK_DIR/valid"
mkdir -p "$VALID"
make_parent_pom "$VALID/group-a" leaf-one leaf-two
make_parent_pom "$VALID/group-b" leaf-three
make_root_pom   "$VALID/pom.xml" '<modules/>' group-a group-b

# Root modules non-empty
ROOT_NONEMPTY="$WORK_DIR/root-nonempty"
mkdir -p "$ROOT_NONEMPTY"
make_parent_pom "$ROOT_NONEMPTY/group-a" leaf-one
make_root_pom   "$ROOT_NONEMPTY/pom.xml" '<modules><module>sneaked-in</module></modules>' group-a

# Missing intermediate parent pom.xml
MISSING_PARENT="$WORK_DIR/missing-parent"
mkdir -p "$MISSING_PARENT"
make_parent_pom "$MISSING_PARENT/group-a" leaf-one
make_root_pom   "$MISSING_PARENT/pom.xml" '<modules/>' group-a group-b   # group-b has no pom.xml

# Empty intermediate parent (no leaf modules)
EMPTY_PARENT="$WORK_DIR/empty-parent"
mkdir -p "$EMPTY_PARENT"
make_parent_pom "$EMPTY_PARENT/group-a" leaf-one
mkdir -p "$EMPTY_PARENT/group-b"
cat > "$EMPTY_PARENT/group-b/pom.xml" <<'XML'
<project xmlns="http://maven.apache.org/POM/4.0.0">
  <modelVersion>4.0.0</modelVersion>
  <groupId>test</groupId><artifactId>empty</artifactId><version>1</version>
  <packaging>pom</packaging>
  <modules/>
</project>
XML
make_root_pom "$EMPTY_PARENT/pom.xml" '<modules/>' group-a group-b

# build-the-world has no modules
BTW_EMPTY="$WORK_DIR/btw-empty"
mkdir -p "$BTW_EMPTY"
cat > "$BTW_EMPTY/pom.xml" <<'XML'
<project xmlns="http://maven.apache.org/POM/4.0.0">
  <modelVersion>4.0.0</modelVersion>
  <groupId>test</groupId><artifactId>root</artifactId><version>1</version>
  <modules/>
  <profiles>
    <profile><id>build-the-world</id><modules/></profile>
  </profiles>
</project>
XML

# ---- Tests ----

assert_passes "valid structure passes"                  "$VALID/pom.xml"
assert_fails  "root modules non-empty fails rule 1"     "$ROOT_NONEMPTY/pom.xml"
assert_fails  "missing intermediate parent fails rule 2" "$MISSING_PARENT/pom.xml"
assert_fails  "empty intermediate parent fails rule 2"   "$EMPTY_PARENT/pom.xml"
assert_fails  "build-the-world with no modules fails"    "$BTW_EMPTY/pom.xml"

# Also run against the real project pom to confirm it passes
REAL_POM="$(cd "$(dirname "$0")/.." && pwd)/pom.xml"
if [ -f "$REAL_POM" ]; then
    assert_passes "real project pom.xml passes" "$REAL_POM"
fi

# ---- Summary ----

echo ""
echo "Results: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
