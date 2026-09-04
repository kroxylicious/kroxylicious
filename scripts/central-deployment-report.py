#!/usr/bin/env python3
#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

"""Report the size and file count of a staged Maven Central Portal deployment.

The Central Portal enforces monthly publishing quotas (release count, total
bytes and total file count). This script inspects a *validated* deployment and
reports, per Maven GAV and in total, how many files it contains and how many
bytes they occupy. It also works out how many such releases fit within the
monthly quotas so you can see which limit is the binding constraint.

How it works
------------
The Portal API exposes no whole-bundle download and no directory listing, only
a per-file endpoint:

    GET /api/v1/publisher/deployment/<id>/download/<relativePath>

So the script first calls the status endpoint to obtain the list of published
package URLs (purls), expands each Maven module into its candidate files
(pom/jar/sources/javadoc plus .asc/.md5/.sha1/.sha256/.sha512 sidecars),
then GETs each candidate to discover which exist and how big they are.

See: https://central.sonatype.org/publish/publish-portal-api/

Authentication
--------------
The bearer token is read from an environment variable (default: CENTRAL_TOKEN)
to keep it out of the process argument list and shell history. The value may
optionally include the leading "Bearer " prefix; it is added if absent. The
token is the Portal user token, base64("username:password"), the same one used
by the central-publishing Maven plugin.

Usage
-----
    export CENTRAL_TOKEN="$(printf 'user:pass' | base64)"

    # JSON (default)
    scripts/central-deployment-report.py <deployment-id-or-url>

    # Human-friendly table
    scripts/central-deployment-report.py --format table <deployment-id-or-url>

The <deployment-id-or-url> may be a bare deployment id, a status URL, or a
download URL - the deployment id is extracted from whichever is given.
"""

import argparse
import json
import os
import re
import sys
import urllib.error
import urllib.request
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import parse_qs, urlparse

DEFAULT_HOST = "https://central.sonatype.com"

# Candidate primary artifact suffixes (appended to "<artifactId>-<version>").
PRIMARY_SUFFIXES = (".pom", ".jar", "-sources.jar", "-javadoc.jar", ".module")
# Candidate checksum/signature sidecars for each primary that exists.
SIDECAR_SUFFIXES = (".asc", ".md5", ".sha1", ".sha256", ".sha512")

# Monthly Central Portal quotas (OSS-exemption values for io.kroxylicious).
DEFAULT_LIMIT_RELEASES = 7
DEFAULT_LIMIT_MB = 850
DEFAULT_LIMIT_FILES = 3960

# A deployment id looks like a UUID.
_UUID_RE = re.compile(
    r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", re.IGNORECASE
)


def extract_deployment_id(value):
    """Extract a deployment id from a bare id, a status URL or a download URL."""
    # status URLs carry the id as a query parameter
    parsed = urlparse(value)
    if parsed.query:
        ids = parse_qs(parsed.query).get("id")
        if ids:
            return ids[0]
    match = _UUID_RE.search(value)
    if not match:
        raise SystemExit(f"Could not find a deployment id (UUID) in: {value!r}")
    return match.group(0)


def bearer_header(token):
    token = token.strip()
    if not token.lower().startswith("bearer "):
        token = "Bearer " + token
    return token


def http_get(url, auth, *, want_body):
    """GET a URL. Returns (status, size_in_bytes). Never raises for 4xx."""
    req = urllib.request.Request(url, headers={"Authorization": auth})
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            if not want_body:
                return resp.status, 0
            size = 0
            while True:
                chunk = resp.read(65536)
                if not chunk:
                    break
                size += len(chunk)
            return resp.status, size
    except urllib.error.HTTPError as exc:
        return exc.code, 0


def fetch_purls(host, deployment_id, auth):
    url = f"{host}/api/v1/publisher/status?id={deployment_id}"
    req = urllib.request.Request(url, method="POST", headers={"Authorization": auth})
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            data = json.load(resp)
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", "replace")[:500]
        raise SystemExit(f"status request failed ({exc.code}): {body}")
    return data


def parse_purl(purl):
    """Parse pkg:maven/<group>/<artifact>@<version>[?type=...] -> (group, artifact, version)."""
    body = purl[len("pkg:maven/"):] if purl.startswith("pkg:maven/") else purl
    if "?" in body:
        body = body.split("?", 1)[0]
    coord, version = body.split("@")
    group, artifact = coord.split("/")
    return group, artifact, version


def unique_gavs(purls):
    """Dedupe purls (pom-packaged modules are listed twice) into GAV tuples."""
    seen = {}
    for purl in purls:
        gav = parse_purl(purl)
        seen[gav] = True
    return sorted(seen)


def gav_path_prefix(group, artifact, version):
    return f"{group.replace('.', '/')}/{artifact}/{version}/{artifact}-{version}"


def enumerate_files(host, deployment_id, auth, gavs, concurrency):
    """Probe every candidate file for every GAV; return {gav: {relpath: size}}."""
    download_base = f"{host}/api/v1/publisher/deployment/{deployment_id}/download"

    candidates = []  # (gav, relpath)
    for gav in gavs:
        prefix = gav_path_prefix(*gav)
        for primary in PRIMARY_SUFFIXES:
            candidates.append((gav, prefix + primary))
            for sidecar in SIDECAR_SUFFIXES:
                candidates.append((gav, prefix + primary + sidecar))

    def probe(item):
        gav, relpath = item
        status, size = http_get(f"{download_base}/{relpath}", auth, want_body=True)
        return gav, relpath, status, size

    present = defaultdict(dict)
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        for gav, relpath, status, size in pool.map(probe, candidates):
            if status == 200:
                present[gav][relpath] = size
    return present


def build_report(deployment_id, status_data, present, limits):
    gav_rows = []
    total_files = 0
    total_bytes = 0
    for gav in sorted(present, key=lambda g: -sum(present[g].values())):
        group, artifact, version = gav
        files = present[gav]
        nbytes = sum(files.values())
        gav_rows.append(
            {
                "gav": f"{group}:{artifact}:{version}",
                "groupId": group,
                "artifactId": artifact,
                "version": version,
                "files": len(files),
                "bytes": nbytes,
            }
        )
        total_files += len(files)
        total_bytes += nbytes

    limit_bytes = limits["mb"] * 1000 * 1000
    by_releases = limits["releases"]
    by_files = limits["files"] // total_files if total_files else None
    by_bytes = limit_bytes // total_bytes if total_bytes else None
    allowances = {
        "releaseCount": by_releases,
        "fileCount": by_files,
        "size": by_bytes,
    }
    binding = min(
        (k for k, v in allowances.items() if v is not None),
        key=lambda k: allowances[k],
    )

    return {
        "deploymentId": deployment_id,
        "deploymentName": status_data.get("deploymentName"),
        "deploymentState": status_data.get("deploymentState"),
        "warnings": status_data.get("warnings", []),
        "totals": {
            "gavs": len(gav_rows),
            "files": total_files,
            "bytes": total_bytes,
        },
        "gavs": gav_rows,
        "monthlyLimits": {
            "releases": limits["releases"],
            "megabytes": limits["mb"],
            "files": limits["files"],
        },
        "releasesPerMonth": {
            "allowedBy": allowances,
            "bindingConstraint": binding,
            "maxReleases": allowances[binding],
        },
    }


def human_bytes(n):
    if n >= 1_000_000:
        return f"{n / 1_000_000:.2f} MB"
    if n >= 1_000:
        return f"{n / 1_000:.1f} kB"
    return f"{n} B"


def print_table(report, stream=sys.stdout):
    rows = report["gavs"]
    name_width = max([len("GAV")] + [len(r["gav"]) for r in rows])
    p = lambda *a: print(*a, file=stream)

    p(f"Deployment: {report['deploymentId']}  ({report.get('deploymentState')})")
    if report.get("warnings"):
        for w in report["warnings"]:
            p(f"  ! {w}")
    p("")
    p(f"{'GAV':<{name_width}}  {'files':>5}  {'size':>12}")
    p("-" * (name_width + 22))
    for r in rows:
        p(f"{r['gav']:<{name_width}}  {r['files']:>5}  {human_bytes(r['bytes']):>12}")
    p("-" * (name_width + 22))
    totals = report["totals"]
    p(
        f"{'TOTAL (' + str(totals['gavs']) + ' GAVs)':<{name_width}}  "
        f"{totals['files']:>5}  {human_bytes(totals['bytes']):>12}  "
        f"({totals['bytes']:,} bytes)"
    )

    limits = report["monthlyLimits"]
    rpm = report["releasesPerMonth"]
    p("")
    p(
        f"Monthly limits: {limits['releases']} releases  "
        f"{limits['megabytes']} MB  {limits['files']} files"
    )
    a = rpm["allowedBy"]
    p(
        f"Releases allowed by:  release-count={a['releaseCount']}  "
        f"files={a['fileCount']}  size={a['size']}"
    )
    p(
        f"=> Binding constraint = {rpm['bindingConstraint']}  "
        f"=> max {rpm['maxReleases']} releases/month"
    )


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Report per-GAV size and file count for a staged Maven Central deployment.",
    )
    parser.add_argument(
        "deployment",
        help="Deployment id, status URL, or download URL (the id is extracted).",
    )
    parser.add_argument(
        "--format",
        choices=("json", "table"),
        default="json",
        help="Output format (default: json).",
    )
    parser.add_argument(
        "--token-env",
        default="CENTRAL_TOKEN",
        metavar="NAME",
        help="Environment variable holding the bearer token (default: CENTRAL_TOKEN).",
    )
    parser.add_argument(
        "--host",
        default=DEFAULT_HOST,
        help=f"Portal base URL (default: {DEFAULT_HOST}).",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=16,
        help="Number of parallel file probes (default: 16).",
    )
    parser.add_argument("--limit-releases", type=int, default=DEFAULT_LIMIT_RELEASES)
    parser.add_argument("--limit-mb", type=int, default=DEFAULT_LIMIT_MB)
    parser.add_argument("--limit-files", type=int, default=DEFAULT_LIMIT_FILES)
    args = parser.parse_args(argv)

    token = os.environ.get(args.token_env)
    if not token:
        raise SystemExit(
            f"Bearer token not found in ${args.token_env}. "
            f"Set it, e.g.: export {args.token_env}=\"$(printf 'user:pass' | base64)\""
        )
    auth = bearer_header(token)

    deployment_id = extract_deployment_id(args.deployment)
    host = args.host.rstrip("/")

    status_data = fetch_purls(host, deployment_id, auth)
    purls = status_data.get("purls", [])
    if not purls:
        raise SystemExit(
            f"Deployment {deployment_id} has no purls "
            f"(state={status_data.get('deploymentState')}). Is it validated?"
        )

    gavs = unique_gavs(purls)
    present = enumerate_files(host, deployment_id, auth, gavs, args.concurrency)

    limits = {
        "releases": args.limit_releases,
        "mb": args.limit_mb,
        "files": args.limit_files,
    }
    report = build_report(deployment_id, status_data, present, limits)

    if args.format == "table":
        print_table(report)
    else:
        json.dump(report, sys.stdout, indent=2)
        sys.stdout.write("\n")


if __name__ == "__main__":
    main()
