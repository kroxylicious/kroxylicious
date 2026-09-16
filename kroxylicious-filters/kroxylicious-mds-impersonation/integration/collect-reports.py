#!/usr/bin/env python3
#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

"""Collect JUnit evidence without publishing JVM properties or captured output."""

import hashlib
import json
import os
import xml.etree.ElementTree as ET
from pathlib import Path


MODULE = Path(__file__).resolve().parent.parent
OUTPUT = MODULE / "target" / "mds-evidence"
LIVE_SUITES = {"MdsConfluentIT", "MdsConfluentRebalanceIT", "MdsConfluentReauthenticationIT"}


def main():
    OUTPUT.mkdir(parents=True, exist_ok=True)
    suites = []
    live_passed = set()
    for report in sorted((MODULE / "target" / "surefire-reports").glob("TEST-*.xml")):
        root = ET.parse(report).getroot()
        name = root.get("name", "")
        if not name.startswith("io.kroxylicious.filter.mds.Mds"):
            continue
        # JVM properties and captured logs can contain credentials/configuration.
        for element in root.iter():
            for child in list(element):
                if child.tag in {"properties", "system-out", "system-err"}:
                    element.remove(child)
        ET.ElementTree(root).write(OUTPUT / report.name, encoding="utf-8", xml_declaration=True)
        counts = {key: int(root.get(key, "0")) for key in ("tests", "failures", "errors", "skipped")}
        suites.append({"name": name, **counts})
        if counts["tests"] > 0 and not any(counts[key] for key in ("failures", "errors", "skipped")):
            live_passed.add(name.rsplit(".", 1)[-1])
    summary = {"suites": suites, "all_live_suites_passed": LIVE_SUITES <= live_passed}
    (OUTPUT / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    lines = ["| Test suite | Tests | Failures | Errors | Skipped |", "| --- | ---: | ---: | ---: | ---: |"]
    for suite in suites:
        lines.append(f"| {suite['name']} | {suite['tests']} | {suite['failures']} | {suite['errors']} | {suite['skipped']} |")
    (OUTPUT / "summary.md").write_text("\n".join(lines) + "\n")
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a", encoding="utf-8") as step_summary:
            step_summary.write("\n".join(lines) + "\n")
    hashes = [f"{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.name}"
              for path in sorted(OUTPUT.iterdir()) if path.is_file() and path.name != "SHA256SUMS"]
    (OUTPUT / "SHA256SUMS").write_text("\n".join(hashes) + "\n")
    if not LIVE_SUITES <= live_passed:
        raise SystemExit("All real Confluent MDS test suites must execute and pass; see the collected reports")


if __name__ == "__main__":
    main()
