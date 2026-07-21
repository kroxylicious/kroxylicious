#!/usr/bin/env python3
#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

"""Validate POM metadata required by Maven Central Portal for all reactor modules.

Maven Central Portal requires each published POM to carry:
  - <name>, <description>, <url>
  - <licenses> with at least one <license> containing <name> and <url>
  - <developers> with at least one <developer> containing <name> or <organization>
  - <scm> with <connection>, <developerConnection>, and <url>

See: https://central.sonatype.org/publish/requirements/

Generate the effective POM first with:

    mvn --batch-mode help:effective-pom -Doutput=target/effective-pom.xml

In a multi-module build Maven writes all effective POMs into that single file
wrapped in a <projects> root element.
"""

import pathlib
import sys
import xml.etree.ElementTree as ET

NS = "http://maven.apache.org/POM/4.0.0"
ns = {"m": NS}


def _text(element, xpath):
    node = element.find(xpath, ns)
    return node.text.strip() if node is not None and node.text else ""


def validate_project(project):
    """Return (artifact_id, [error_messages]) for one <project> element."""
    artifact_id = _text(project, "m:artifactId") or "(unknown)"
    errors = []

    for field in ("name", "description", "url"):
        if not _text(project, f"m:{field}"):
            errors.append(f"<{field}> is missing or empty")

    licenses = project.find("m:licenses", ns)
    if licenses is None:
        errors.append("<licenses> is missing")
    else:
        valid = any(
            _text(lic, "m:name") and _text(lic, "m:url")
            for lic in licenses.findall("m:license", ns)
        )
        if not valid:
            errors.append("<licenses> has no <license> with both <name> and <url>")

    developers = project.find("m:developers", ns)
    if developers is None:
        errors.append("<developers> is missing")
    else:
        valid = any(
            _text(dev, "m:name") or _text(dev, "m:organization")
            for dev in developers.findall("m:developer", ns)
        )
        if not valid:
            errors.append("<developers> has no <developer> with <name> or <organization>")

    scm = project.find("m:scm", ns)
    if scm is None:
        errors.append("<scm> is missing")
    else:
        for field in ("connection", "developerConnection", "url"):
            if not _text(scm, f"m:{field}"):
                errors.append(f"<scm><{field}> is missing or empty")

    return artifact_id, errors


def main():
    pom_file = pathlib.Path(sys.argv[1]) if len(sys.argv) > 1 else pathlib.Path("target/effective-pom.xml")
    expected_count = int(sys.argv[2]) if len(sys.argv) > 2 else None
    if not pom_file.exists():
        print(
            f"{pom_file} not found.\n"
            f"Run: mvn --batch-mode help:effective-pom -Doutput={pom_file}",
            file=sys.stderr,
        )
        sys.exit(1)

    try:
        tree = ET.parse(pom_file)
    except ET.ParseError as e:
        print(f"Failed to parse {pom_file}: {e}", file=sys.stderr)
        sys.exit(1)

    root = tree.getroot()

    # Multi-module: <projects> wrapper (no namespace) containing <project> children.
    # Single-module: root is <project> directly.
    project_tag = f"{{{NS}}}project"
    if root.tag == project_tag:
        projects = [root]
    else:
        projects = root.findall(project_tag)

    if not projects:
        print(f"No <project> elements found in {pom_file}", file=sys.stderr)
        sys.exit(1)

    failures = []
    for project in projects:
        artifact_id, errors = validate_project(project)
        if errors:
            failures.append((artifact_id, errors))

    if expected_count is not None and len(projects) != expected_count:
        print(
            f"POM metadata validation FAILED: expected {expected_count} module(s) but found {len(projects)}.\n"
            "A module may have been added or removed from the reactor.",
            file=sys.stderr,
        )
        sys.exit(1)

    if failures:
        print(f"POM metadata validation FAILED for {len(failures)} module(s):\n")
        for artifact_id, errors in failures:
            print(f"  {artifact_id}:")
            for error in errors:
                print(f"    - {error}")
        sys.exit(1)

    print(f"POM metadata validation passed: {len(projects)} module(s) checked.")


if __name__ == "__main__":
    main()
