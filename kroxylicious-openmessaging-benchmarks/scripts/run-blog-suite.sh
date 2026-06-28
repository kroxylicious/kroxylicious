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
#   Post 2 — CPU coefficient sweep (run-coefficient-sweep: 1/2/4 cores × 1/10/100 topics, RF=1)
#   Post 2 — Connection sweep, proxy-no-filters, 4-core, 10-topics, RF=1

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
MODULE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

cd "${MODULE_DIR}"

usage() {
    cat >&2 <<EOF
Usage: $(basename "$0") [--cluster-overrides <file>] [--output-dir <dir>]

Options:
  --cluster-overrides <file>  Helm values for cluster-specific settings (e.g. vault image)
  --output-dir <dir>          Root directory for all results
                              (default: results/blog-suite-<timestamp>)
  -h, --help                  Show this help
EOF
    exit 1
}

CLUSTER_OVERRIDES=""
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
echo "Cluster overrides: ${CLUSTER_OVERRIDES:-none}"
echo "Started: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "=========================================="
echo ""

CLUSTER_OVERRIDES_ARG=()
[[ -n "${CLUSTER_OVERRIDES}" ]] && CLUSTER_OVERRIDES_ARG=(--cluster-overrides "${CLUSTER_OVERRIDES}")

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

# Proxy resource presets — passed as --set flags so cluster-overrides stays
# cluster-specific (vault image, storage class) rather than run-specific.
# Memory is held constant across core counts so only CPU varies between runs.
PROXY_MEM="--set kroxylicious.resources.requests.memory=2Gi --set kroxylicious.resources.limits.memory=4Gi"
PROXY_1CORE="--set kroxylicious.resources.requests.cpu=1000m --set kroxylicious.resources.limits.cpu=1000m ${PROXY_MEM}"
PROXY_4CORE="--set kroxylicious.resources.requests.cpu=4000m --set kroxylicious.resources.limits.cpu=4000m ${PROXY_MEM}"

# ---------------------------------------------------------------------------
# Post 1 — Multi-topic latency (RF=3, 1-core proxy, 3 scenarios × 3 workloads)
# ---------------------------------------------------------------------------
run_step "post1-multi-topic-latency" \
    scripts/run-all-scenarios.sh \
        baseline proxy-no-filters encryption \
        ${CLUSTER_OVERRIDES_ARG[@]+"${CLUSTER_OVERRIDES_ARG[@]}"} \
        ${PROXY_1CORE} \
        "${OUTPUT_DIR}/all-scenarios/"

# ---------------------------------------------------------------------------
# Post 1 — Rate sweep (RF=3, 1-core proxy, 8k–22k msg/s, 5% steps)
# ---------------------------------------------------------------------------
run_step "post1-rate-sweep-1core-rf3" \
    scripts/rate-sweep.sh \
        --min-rate 8000 --max-rate 22000 --step-percent 5 \
        --scenarios baseline,proxy-no-filters,encryption \
        --workload 1topic-1kb \
        ${CLUSTER_OVERRIDES_ARG[@]+"${CLUSTER_OVERRIDES_ARG[@]}"} \
        ${PROXY_1CORE} \
        --output-dir "${OUTPUT_DIR}/rate-sweep-1core-rf3/"

# ---------------------------------------------------------------------------
# Post 2 — CPU coefficient sweep (1/2/4 cores × 1/10/100 topics, all RF=1)
# ---------------------------------------------------------------------------
run_step "post2-cpu-coefficient-sweep" \
    scripts/run-coefficient-sweep.sh \
        ${CLUSTER_OVERRIDES_ARG[@]+"${CLUSTER_OVERRIDES_ARG[@]}"} \
        --output-dir "${OUTPUT_DIR}/coefficient-sweep/"

# ---------------------------------------------------------------------------
# Post 2 — Proxy overhead baseline (no filters, 4-core, 10-topics, RF=1)
# ---------------------------------------------------------------------------
run_step "post2-conn-sweep-no-filters-4core-10topics" \
    scripts/connection-sweep.sh \
        --scenario proxy-no-filters \
        --per-producer-rate 10000 \
        --steps 1,2,4,8,16,32 \
        --workload 10topics-1kb \
        ${CLUSTER_OVERRIDES_ARG[@]+"${CLUSTER_OVERRIDES_ARG[@]}"} \
        --set kafka.replicationFactor=1 \
        --set kafka.minInSyncReplicas=1 \
        ${PROXY_4CORE} \
        --output-dir "${OUTPUT_DIR}/conn-sweep-no-filters-4core-10topics-rf1/"

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
