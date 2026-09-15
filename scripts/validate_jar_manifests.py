#!/usr/bin/env python3
#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

"""Validate implementation metadata in a packaged JAR manifest."""

import argparse
import sys
import zipfile
from pathlib import Path


IMPLEMENTATION_ATTRIBUTES = {
    "Implementation-Version": "expected_version",
    "Implementation-Title": "expected_title",
    "Implementation-Vendor": "expected_vendor",
}


def parse_manifest(manifest_content):
    """Parse the main section of a JAR manifest, including continued lines."""
    attributes = {}
    current_key = None
    current_value = []

    for line in manifest_content.splitlines():
        if not line:
            if current_key is not None:
                attributes[current_key] = "".join(current_value)
            break

        if line.startswith(" "):
            if current_key is not None:
                current_value.append(line[1:])
            continue

        if current_key is not None:
            attributes[current_key] = "".join(current_value)

        key, separator, value = line.partition(":")
        if not separator:
            current_key = None
            current_value = []
            continue

        current_key = key
        current_value = [value.lstrip()]
    else:
        if current_key is not None:
            attributes[current_key] = "".join(current_value)

    return attributes


def validate_manifest(
    jar_path,
    expected_version=None,
    expected_title=None,
    expected_vendor=None,
):
    """Return validation errors for the implementation metadata in jar_path."""
    if not jar_path.exists():
        return [f"JAR file not found: {jar_path}"]

    try:
        with zipfile.ZipFile(jar_path) as jar:
            manifest_content = jar.read("META-INF/MANIFEST.MF").decode("utf-8")
    except (OSError, KeyError, UnicodeDecodeError, zipfile.BadZipFile) as error:
        return [f"Failed to read manifest from {jar_path}: {error}"]

    attributes = parse_manifest(manifest_content)
    expected_values = {
        "expected_version": expected_version,
        "expected_title": expected_title,
        "expected_vendor": expected_vendor,
    }
    errors = []

    for attribute, expected_name in IMPLEMENTATION_ATTRIBUTES.items():
        actual = attributes.get(attribute)
        if not actual:
            errors.append(f"Missing or empty manifest attribute: {attribute}")
            continue

        expected = expected_values[expected_name]
        if expected is not None and actual != expected:
            errors.append(
                f"{attribute} mismatch: expected '{expected}', got '{actual}'"
            )

    return errors


def main():
    parser = argparse.ArgumentParser(
        description="Validate JAR manifest implementation entries"
    )
    parser.add_argument("jar_file", type=Path, help="Path to JAR file to validate")
    parser.add_argument("--expected-version")
    parser.add_argument("--expected-title")
    parser.add_argument("--expected-vendor")
    args = parser.parse_args()

    errors = validate_manifest(
        args.jar_file,
        expected_version=args.expected_version,
        expected_title=args.expected_title,
        expected_vendor=args.expected_vendor,
    )

    if errors:
        print(f"Manifest validation FAILED for {args.jar_file.name}:", file=sys.stderr)
        for error in errors:
            print(f"  - {error}", file=sys.stderr)
        sys.exit(1)

    print(f"Manifest validation passed: {args.jar_file.name}")


if __name__ == "__main__":
    main()
