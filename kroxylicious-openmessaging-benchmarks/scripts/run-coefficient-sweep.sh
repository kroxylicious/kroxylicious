#!/usr/bin/env bash
#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

set -uo pipefail

# Runs a systematic CPU-coefficient validation sweep across proxy core counts and topic counts.
#
# Runs 9 connection sweeps (1/2/4 cores × 1/10/100 topics) then prints a
# cross-configuration comparison table so you can verify the coefficient is
# consistent across proxy sizes.
#
# Usage: scripts/run-coefficient-sweep.sh [--cluster-overrides <file>] [--output-dir <dir>]
#
# Run from: kroxylicious-openmessaging-benchmarks/
#
# Estimated runtime: ~9–18 hours depending on cluster speed.

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
MODULE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

cd "${MODULE_DIR}"

usage() {
    cat >&2 <<EOF
Usage: $(basename "$0") [--cluster-overrides <file>] [--output-dir <dir>]

Options:
  --cluster-overrides <file>  Helm values for cluster-specific settings
  --output-dir <dir>          Root directory for all results
                              (default: results/coefficient-sweep-<timestamp>)
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
OUTPUT_DIR="${OUTPUT_DIR:-results/coefficient-sweep-${RUN_ID}}"
mkdir -p "${OUTPUT_DIR}"

exec > >(tee "${OUTPUT_DIR}/suite.log") 2>&1

echo "=========================================="
echo "CPU coefficient sweep — ${RUN_ID}"
echo "Results: ${OUTPUT_DIR}"
echo "Cluster overrides: ${CLUSTER_OVERRIDES:-none}"
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

PROXY_MEM="--set kroxylicious.resources.requests.memory=2Gi --set kroxylicious.resources.limits.memory=4Gi"
PROXY_1CORE="--set kroxylicious.resources.requests.cpu=1000m --set kroxylicious.resources.limits.cpu=1000m ${PROXY_MEM}"
PROXY_2CORE="--set kroxylicious.resources.requests.cpu=2000m --set kroxylicious.resources.limits.cpu=2000m ${PROXY_MEM}"
PROXY_4CORE="--set kroxylicious.resources.requests.cpu=4000m --set kroxylicious.resources.limits.cpu=4000m ${PROXY_MEM}"

CLUSTER_OVERRIDES_ARG=()
[[ -n "${CLUSTER_OVERRIDES}" ]] && CLUSTER_OVERRIDES_ARG=(--cluster-overrides "${CLUSTER_OVERRIDES}")

COMMON=(
    --scenario encryption
    --per-producer-rate 10000
    --steps 1,2,4,8,16,32
    ${CLUSTER_OVERRIDES_ARG[@]+"${CLUSTER_OVERRIDES_ARG[@]}"}
    --set kafka.replicationFactor=1
    --set kafka.minInSyncReplicas=1
)

# ---------------------------------------------------------------------------
# 1-topic sweeps
# ---------------------------------------------------------------------------
run_step "coeff-1core-1topic" \
    scripts/connection-sweep.sh "${COMMON[@]}" --workload 1topic-1kb \
        ${PROXY_1CORE} \
        --output-dir "${OUTPUT_DIR}/conn-sweep-encryption-1core-1topic-rf1/"

run_step "coeff-2core-1topic" \
    scripts/connection-sweep.sh "${COMMON[@]}" --workload 1topic-1kb \
        ${PROXY_2CORE} \
        --output-dir "${OUTPUT_DIR}/conn-sweep-encryption-2core-1topic-rf1/"

run_step "coeff-4core-1topic" \
    scripts/connection-sweep.sh "${COMMON[@]}" --workload 1topic-1kb \
        ${PROXY_4CORE} \
        --output-dir "${OUTPUT_DIR}/conn-sweep-encryption-4core-1topic-rf1/"

# ---------------------------------------------------------------------------
# 10-topic sweeps
# ---------------------------------------------------------------------------
run_step "coeff-1core-10topics" \
    scripts/connection-sweep.sh "${COMMON[@]}" --workload 10topics-1kb \
        ${PROXY_1CORE} \
        --output-dir "${OUTPUT_DIR}/conn-sweep-encryption-1core-10topics-rf1/"

run_step "coeff-2core-10topics" \
    scripts/connection-sweep.sh "${COMMON[@]}" --workload 10topics-1kb \
        ${PROXY_2CORE} \
        --output-dir "${OUTPUT_DIR}/conn-sweep-encryption-2core-10topics-rf1/"

run_step "coeff-4core-10topics" \
    scripts/connection-sweep.sh "${COMMON[@]}" --workload 10topics-1kb \
        ${PROXY_4CORE} \
        --output-dir "${OUTPUT_DIR}/conn-sweep-encryption-4core-10topics-rf1/"

# ---------------------------------------------------------------------------
# 100-topic sweeps
# ---------------------------------------------------------------------------
run_step "coeff-1core-100topics" \
    scripts/connection-sweep.sh "${COMMON[@]}" --workload 100topics-1kb \
        ${PROXY_1CORE} \
        --output-dir "${OUTPUT_DIR}/conn-sweep-encryption-1core-100topics-rf1/"

run_step "coeff-2core-100topics" \
    scripts/connection-sweep.sh "${COMMON[@]}" --workload 100topics-1kb \
        ${PROXY_2CORE} \
        --output-dir "${OUTPUT_DIR}/conn-sweep-encryption-2core-100topics-rf1/"

run_step "coeff-4core-100topics" \
    scripts/connection-sweep.sh "${COMMON[@]}" --workload 100topics-1kb \
        ${PROXY_4CORE} \
        --output-dir "${OUTPUT_DIR}/conn-sweep-encryption-4core-100topics-rf1/"

# ---------------------------------------------------------------------------
# Cross-configuration comparison
# ---------------------------------------------------------------------------
echo ""
echo "--- CPU coefficient comparison ---"

for topics_dir in 1topic 10topics 100topics; do
    label="${topics_dir/topic/ topic}"
    label="${label/topics/ topics}"
    echo ""
    echo "=== ${label} ==="
    compare_args=()
    for cores in 1 2 4; do
        dir="${OUTPUT_DIR}/conn-sweep-encryption-${cores}core-${topics_dir}-rf1/encryption"
        if [[ -d "${dir}" ]]; then
            compare_args+=("${cores}-core-${topics_dir}:${dir}")
        fi
    done
    if [[ ${#compare_args[@]} -gt 0 ]]; then
        python3 scripts/analyze-cpu-coefficient.py --compare "${compare_args[@]}"
    else
        echo "  (no completed sweeps)"
    fi
done

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
