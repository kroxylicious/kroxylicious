#!/usr/bin/env python3
#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

"""Provision a local topic and alice's RBAC binding without exposing bearer tokens."""

import argparse
import json
import ssl
import subprocess
import time
import urllib.error
import urllib.request
from pathlib import Path


BASE = Path(__file__).resolve().parent
GENERATED = BASE / "generated"
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--project-name", default="kroxy-mds-integration")
args = parser.parse_args()
context = ssl.create_default_context(cafile=str(GENERATED / "server-ca.crt"))
context.load_cert_chain(str(GENERATED / "admin.crt"), str(GENERATED / "admin.key"))
opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), urllib.request.HTTPSHandler(context=context))
binding = {
    "scope": {"clusters": {"kafka-cluster": "YmJ5YS1tZHMtdGVzdC1jbA"}},
    "resourcePatterns": [
        {"resourceType": "Topic", "name": "mds-allowed", "patternType": "LITERAL"},
        {"resourceType": "Group", "name": "mds-test", "patternType": "LITERAL"},
    ],
}
request = urllib.request.Request(
    "https://localhost:18090/security/1.0/principals/User:alice/roles/ResourceOwner/bindings",
    data=json.dumps(binding).encode(), headers={"Content-Type": "application/json"}, method="POST")
for attempt in range(30):
    try:
        with opener.open(request, timeout=2) as response:
            print("MDS RBAC binding:", response.status)
        break
    except (urllib.error.URLError, TimeoutError):
        if attempt == 29:
            raise
        time.sleep(1)
subprocess.run(["docker", "compose", "-p", args.project_name, "exec", "-T", "broker", "kafka-topics",
                "--bootstrap-server", "broker:9091", "--create", "--if-not-exists", "--topic", "mds-allowed",
                "--partitions", "2", "--replication-factor", "1"], cwd=BASE, check=True)
(GENERATED / "proxy.yaml").write_text((BASE / "proxy.yaml.in").read_text().replace("{{GENERATED}}", str(GENERATED)))
