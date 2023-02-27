#!/usr/bin/env bash
#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
ENTRY_FILE=$1
OUTPUT_FILE=${2:-output.json}
STATS=$(tail -n 1 "$ENTRY_FILE")

# STATS format: 10000000 records sent, 62987.761478 records/sec (61.51 MB/sec), 13.64 ms avg latency, 1301.00 ms max latency, 0 ms 50th, 27 ms 95th, 332 ms 99th, 1285 ms 99.9th.

IFS=',' read -ra LATENCY_STATS <<< "$STATS"
IFS=' ' read -ra LATENCY_AVG <<< "${LATENCY_STATS[2]}"
LATENCY_AVG_MS=${LATENCY_AVG[0]}
IFS=' ' read -ra LATENCY_95TH <<< "${LATENCY_STATS[5]}"
LATENCY_95TH_MS=${LATENCY_95TH[0]}
IFS=' ' read -ra LATENCY_99TH <<< "${LATENCY_STATS[6]}"
LATENCY_99TH_MS=${LATENCY_99TH[0]}

JSON_FILE="[ {\"name\": \"AVG Latency\", \"unit\": \"ms\", \"value\": $LATENCY_AVG_MS},
{\"name\": \"95th Latency\", \"unit\": \"ms\", \"value\": $LATENCY_95TH_MS},
{\"name\": \"99.9th Latency\", \"unit\": \"ms\", \"value\": $LATENCY_99TH_MS}]"

echo "$JSON_FILE" | jq . > "$DIR/$OUTPUT_FILE"
