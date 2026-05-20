#!/usr/bin/env bash
#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

set -uo pipefail

# Runs the full benchmark suite for the Kroxylicious blog posts.
#
# Executes all steps sequentially and continues on failure, reporting a summary
# at the end. Designed to be kicked off in a tmux session and left running
# (~14 hours total).
#
# Usage: scripts/run-blog-suite.sh [--cluster-overrides <file>] [--output-dir <dir>]
#
# Run from: kroxylicious-openmessaging-benchmarks/
#
# Steps:
#   Post 1 — Multi-topic latency  (run-all-scenarios, RF=3, 3 scenarios × 3 workloads)
#   Post 1 — Rate sweep           (rate-sweep, RF=3, 1-core proxy, 8k–22k msg/s, 5% steps)
#   Post 2 — Connection sweep, encryption, 1-core, 1-topic, RF=1
#   Post 2 — Connection sweep, encryption, 4-core, 1-topic, RF=1
#   Post 2 — Connection sweep, encryption, 4-core, 10-topics, RF=1
#   Post 2 — Connection sweep, proxy-no-filters, 4-core, 10-topics, RF=1
#   Post 2 — CPU coefficient analysis (4-core sweeps)

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
MODULE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

cd "${MODULE_DIR}"

usage() {
    cat >&2 <<EOF
Usage: $(basename "$0") [--cluster-overrides <file>] [--output-dir <dir>]

Options:
  --cluster-overrides <file>  Helm values for cluster-specific settings
                              (default: fyre-cluster.yaml)
  --output-dir <dir>          Root directory for all results
                              (default: results/blog-suite-<timestamp>)
  -h, --help                  Show this help
EOF
    exit 1
}

CLUSTER_OVERRIDES="fyre-cluster.yaml"
OUTPUT_DIR=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --cluster-overrides) CLUSTER_OVERRIDES="$2"; shift 2 ;;
        --output-dir)        OUTPUT_DIR="$2";        shift 2 ;;
        -h|--help)           usage ;;
        *) echo "Error: unknown argument '$1'" >&2; usage ;;
    esac
done

RUN_ID="$(date +%Y%m%d-%H%M%S)"
OUTPUT_DIR="${OUTPUT_DIR:-results/blog-suite-${RUN_ID}}"
mkdir -p "${OUTPUT_DIR}"

# Tee all output to a log file — tail -f results/blog-suite-<id>/suite.log to monitor
exec > >(tee "${OUTPUT_DIR}/suite.log") 2>&1

echo "=========================================="
echo "Blog benchmark suite — ${RUN_ID}"
echo "Results: ${OUTPUT_DIR}"
echo "Cluster overrides: ${CLUSTER_OVERRIDES}"
echo "Started: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "=========================================="
echo ""

FAILED_STEPS=()

run_step() {
    local name="$1"
    shift
    local start
    start="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo ""
    echo "--- ${name} ---"
    echo "Started: ${start}"
    if "$@"; then
        echo "Completed: $(date -u +%Y-%m-%dT%H:%M:%SZ) — ${name}"
    else
        echo "FAILED: $(date -u +%Y-%m-%dT%H:%M:%SZ) — ${name}" >&2
        FAILED_STEPS+=("${name}")
    fi
}

# ---------------------------------------------------------------------------
# Post 1 — Multi-topic latency (RF=3, 3 scenarios × 3 workloads, ~3 hours)
# ---------------------------------------------------------------------------
run_step "post1-multi-topic-latency" \
    scripts/run-all-scenarios.sh \
        baseline proxy-no-filters encryption \
        --cluster-overrides "${CLUSTER_OVERRIDES}" \
        "${OUTPUT_DIR}/all-scenarios/"

# ---------------------------------------------------------------------------
# Post 1 — Rate sweep (RF=3, 1-core proxy, 8k–22k msg/s, 5% steps, ~10 hours)
# ---------------------------------------------------------------------------
run_step "post1-rate-sweep-1core-rf3" \
    scripts/rate-sweep.sh \
        --min-rate 8000 --max-rate 22000 --step-percent 5 \
        --scenarios baseline,proxy-no-filters,encryption \
        --workload 1topic-1kb \
        --cluster-overrides "${CLUSTER_OVERRIDES}" \
        --set kroxylicious.resources.requests.cpu=1000m \
        --set kroxylicious.resources.limits.cpu=1000m \
        --output-dir "${OUTPUT_DIR}/rate-sweep-1core-rf3/"

# ---------------------------------------------------------------------------
# Post 2 — Connection sweeps (all RF=1, 10k msg/s per producer)
# ---------------------------------------------------------------------------
run_step "post2-conn-sweep-encryption-1core-1topic" \
    scripts/connection-sweep.sh \
        --scenario encryption \
        --per-producer-rate 10000 \
        --steps 1,2,4,8,16 \
        --workload 1topic-1kb \
        --cluster-overrides "${CLUSTER_OVERRIDES}" \
        --set kafka.replicationFactor=1 \
        --set kafka.minInSyncReplicas=1 \
        --set kroxylicious.resources.requests.cpu=1000m \
        --set kroxylicious.resources.limits.cpu=1000m \
        --output-dir "${OUTPUT_DIR}/conn-sweep-encryption-1core-1topic-rf1/"

run_step "post2-conn-sweep-encryption-4core-1topic" \
    scripts/connection-sweep.sh \
        --scenario encryption \
        --per-producer-rate 10000 \
        --steps 1,2,4,8,16,32 \
        --workload 1topic-1kb \
        --cluster-overrides "${CLUSTER_OVERRIDES}" \
        --set kafka.replicationFactor=1 \
        --set kafka.minInSyncReplicas=1 \
        --output-dir "${OUTPUT_DIR}/conn-sweep-encryption-4core-1topic-rf1/"

run_step "post2-conn-sweep-encryption-4core-10topics" \
    scripts/connection-sweep.sh \
        --scenario encryption \
        --per-producer-rate 10000 \
        --steps 1,2,4,8,16,32 \
        --workload 10topics-1kb \
        --cluster-overrides "${CLUSTER_OVERRIDES}" \
        --set kafka.replicationFactor=1 \
        --set kafka.minInSyncReplicas=1 \
        --output-dir "${OUTPUT_DIR}/conn-sweep-encryption-4core-10topics-rf1/"

run_step "post2-conn-sweep-no-filters-4core-10topics" \
    scripts/connection-sweep.sh \
        --scenario proxy-no-filters \
        --per-producer-rate 10000 \
        --steps 1,2,4,8,16,32 \
        --workload 10topics-1kb \
        --cluster-overrides "${CLUSTER_OVERRIDES}" \
        --set kafka.replicationFactor=1 \
        --set kafka.minInSyncReplicas=1 \
        --output-dir "${OUTPUT_DIR}/conn-sweep-no-filters-4core-10topics-rf1/"

# ---------------------------------------------------------------------------
# Post 2 — CPU coefficient (run against 4-core sweeps if they succeeded)
# ---------------------------------------------------------------------------
COEFF_1TOPIC="${OUTPUT_DIR}/conn-sweep-encryption-4core-1topic-rf1/encryption"
COEFF_10TOPICS="${OUTPUT_DIR}/conn-sweep-encryption-4core-10topics-rf1/encryption"

if [[ -d "${COEFF_1TOPIC}" ]]; then
    run_step "post2-cpu-coefficient-1topic" \
        python3 scripts/analyze-cpu-coefficient.py "${COEFF_1TOPIC}"
else
    echo "Skipping cpu-coefficient-1topic — sweep directory not found: ${COEFF_1TOPIC}"
fi

if [[ -d "${COEFF_10TOPICS}" ]]; then
    run_step "post2-cpu-coefficient-10topics" \
        python3 scripts/analyze-cpu-coefficient.py "${COEFF_10TOPICS}"
else
    echo "Skipping cpu-coefficient-10topics — sweep directory not found: ${COEFF_10TOPICS}"
fi

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
echo ""
echo "=========================================="
echo "Suite complete: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "Results: ${OUTPUT_DIR}"
echo "=========================================="

if [[ ${#FAILED_STEPS[@]} -gt 0 ]]; then
    echo ""
    echo "Failed steps:"
    for step in ${FAILED_STEPS[@]+"${FAILED_STEPS[@]}"}; do
        echo "  - ${step}"
    done
    exit 1
fi

echo "All steps succeeded."
