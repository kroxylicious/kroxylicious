#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

# Test plugin OCI image for integration tests.
# Plugin JARs go in /plugins/ — the webhook mounts this subdirectory
# (via subPath for OCI, or copies from it for emptyDir fallback).
# Uses busybox (not scratch) so the emptyDir fallback init container
# can run "sh -c cp" when OCI image volumes are unavailable.
FROM quay.io/quay/busybox
COPY target/test-plugin-jars/*.jar /plugins/
