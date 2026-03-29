#!/usr/bin/env bash
#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

# Exits 0 if the end-to-end p99 latency delta between two OMB result JSON files is
# statistically significant (MWU p < 0.05), or 1 if not.
# Used by rate-sweep.sh to annotate the summary table with noise callouts.

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
MODULE_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
FILTERED="${MODULE_DIR}/target/jbang/generated-sources/io/kroxylicious/benchmarks/results/CheckSignificance.java"

if [[ ! -f "$FILTERED" ]]; then
    echo "Error: run 'mvn process-sources -pl kroxylicious-openmessaging-benchmarks' first to generate filtered sources" >&2
    exit 2
fi

exec jbang "$FILTERED" "$@"
