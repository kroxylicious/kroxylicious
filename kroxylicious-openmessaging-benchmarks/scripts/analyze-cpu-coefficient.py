#!/usr/bin/env python3
#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

"""
Derive a proxy CPU sizing coefficient from connection-sweep results.

For each probe in a connection-sweep output directory, reads:
  - result.json        → achieved publish rate (msg/s) and message size (bytes)
  - run-metadata.json  → benchmark phase timestamps and workload parameters
  - proxy-metrics.txt  → timestamped process_cpu_usage snapshots (Prometheus text format)

CPU snapshots are filtered to the measurement phase only using the timestamps in
run-metadata.json (benchmarkStartedAt + warmupDurationMinutes → testStartedAt,
benchmarkCompletedAt → testEndedAt). Falls back to --skip-first if timestamps
are absent.

Computes the coefficient:

    coeff = proxy_cpu_millicores / total_throughput_MB_per_s

where total_throughput_MB/s = achieved_rate × message_size_bytes × 2 (bidirectional)
      proxy_cpu_millicores  = avg(process_cpu_usage during test phase) × 1000

The coefficient units are "millicores per MB/s of bidirectional traffic".
An operator can then estimate required proxy CPU as:

    CPU_millicores = coeff × (produce_MB_per_s + consume_MB_per_s)

Usage:
    python3 analyze-cpu-coefficient.py <sweep-output-dir> [--skip-first N]

    sweep-output-dir  Root of a connection-sweep run, e.g. /tmp/results/conn-sweep/
    --skip-first N    Fallback: skip the first N CPU snapshots per probe when
                      phase timestamps are absent (default: 10)

Only non-saturated probes (achieved >= 95% of target rate) are used to compute
the final coefficient, since at saturation the CPU is capped and the ratio breaks.
"""

import argparse
import glob
import json
import os
import re
import statistics
import subprocess
import sys
from datetime import datetime, timedelta, timezone


def parse_cpu_snapshots(metrics_file):
    """Return list of (datetime, cpu_fraction) pairs from proxy-metrics.txt SNAPSHOT blocks."""
    snapshots = []
    current_dt = None
    current_cpu = None
    with open(metrics_file) as f:
        for line in f:
            line = line.rstrip()
            if line.startswith("# SNAPSHOT"):
                if current_dt is not None and current_cpu is not None:
                    snapshots.append((current_dt, current_cpu))
                current_dt = None
                current_cpu = None
                m = re.search(r"datetime=(\S+)", line)
                if m:
                    try:
                        current_dt = datetime.fromisoformat(
                            m.group(1).replace("Z", "+00:00")
                        )
                    except ValueError:
                        pass
            elif line.startswith("process_cpu_usage"):
                parts = line.split()
                if len(parts) >= 2:
                    try:
                        current_cpu = float(parts[1])
                    except ValueError:
                        pass
    if current_dt is not None and current_cpu is not None:
        snapshots.append((current_dt, current_cpu))
    return snapshots


def parse_jfr_cpu_snapshots(jfr_file, node_cpus):
    """Return list of (datetime, cpu_fraction) from jdk.CPULoad events in a JFR recording.

    jdk.CPULoad reports jvmUser and jvmSystem as a percentage of all machine CPUs.
    We sum them and divide by node_cpus to normalise to a fraction of one container CPU,
    matching the scale of process_cpu_usage from proxy-metrics.txt.
    """
    try:
        result = subprocess.run(
            ["jfr", "print", "--events", "jdk.CPULoad", jfr_file],
            capture_output=True, text=True, timeout=120,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return []

    snapshots = []
    current_dt = None
    u = s = 0.0
    for line in result.stdout.splitlines():
        m = re.search(r"startTime = (\d+:\d+:\d+\.\d+) \((\d+-\d+-\d+)\)", line)
        if m:
            current_dt = datetime.strptime(
                f"{m.group(2)} {m.group(1)}", "%Y-%m-%d %H:%M:%S.%f"
            ).replace(tzinfo=timezone.utc)
            u = s = 0.0
        elif "jvmUser" in line:
            mv = re.search(r"([\d.]+)%", line)
            if mv:
                u = float(mv.group(1))
        elif "jvmSystem" in line:
            mv = re.search(r"([\d.]+)%", line)
            if mv:
                s = float(mv.group(1))
                if current_dt is not None:
                    # Convert from % of machine CPUs to fraction of one CPU
                    cpu_fraction = (u + s) / 100.0 * node_cpus
                    snapshots.append((current_dt, cpu_fraction))
    return snapshots


def filter_snapshots(snapshots, meta, skip_first):
    """Return CPU values for the measurement phase only."""
    started_at = meta.get("benchmarkStartedAt")
    completed_at = meta.get("benchmarkCompletedAt")
    warmup_minutes = meta.get("warmupDurationMinutes")

    if started_at and completed_at and warmup_minutes is not None:
        try:
            t_start = datetime.fromisoformat(started_at.replace("Z", "+00:00"))
            t_end = datetime.fromisoformat(completed_at.replace("Z", "+00:00"))
            t_test_start = t_start + timedelta(minutes=warmup_minutes)
            values = [
                cpu for dt, cpu in snapshots
                if t_test_start <= dt <= t_end
            ]
            if values:
                return values, "timestamp"
        except (ValueError, TypeError):
            pass

    # Fallback: skip first N snapshots
    values = [cpu for _, cpu in snapshots[skip_first:]]
    return values or [cpu for _, cpu in snapshots], "skip-first"


def load_probe(probe_dir, skip_first):
    """Load result.json, run-metadata.json, and CPU source from a probe directory.

    CPU source priority: proxy-metrics.txt (process_cpu_usage) → JFR (jdk.CPULoad).
    """
    result_path = os.path.join(probe_dir, "result.json")
    metrics_path = os.path.join(probe_dir, "proxy-metrics.txt")
    meta_path = os.path.join(probe_dir, "run-metadata.json")

    if not os.path.exists(result_path):
        return None

    with open(result_path) as f:
        result = json.load(f)

    rates = result.get("publishRate", [])
    if not rates:
        return None

    meta = {}
    if os.path.exists(meta_path):
        with open(meta_path) as f:
            meta = json.load(f)

    # Try proxy-metrics.txt first
    cpu_source = "none"
    snapshots = []
    if os.path.exists(metrics_path):
        snapshots = parse_cpu_snapshots(metrics_path)
        if snapshots:
            cpu_source = "metrics"

    # Fall back to JFR if no process_cpu_usage in metrics
    if not snapshots:
        jfr_files = glob.glob(os.path.join(probe_dir, "*.jfr"))
        if jfr_files:
            node_cpus = int(meta.get("clusterNodes", {}).get("cpuPerNode", 1))
            snapshots = parse_jfr_cpu_snapshots(jfr_files[0], node_cpus)
            if snapshots:
                cpu_source = f"jfr({node_cpus}cpu)"

    if not snapshots:
        return None

    usable, filter_method = filter_snapshots(snapshots, meta, skip_first)

    return {
        "achieved_rate": sum(rates) / len(rates),
        "message_size": meta.get("messageSize") or result.get("messageSize", 1024),
        "producers": meta.get("producersPerTopic") or result.get("producersPerTopic", 1),
        "topics": meta.get("topics", 1),
        "target": meta.get("targetRate"),
        "all_snapshots": len(snapshots),
        "usable_snapshots": usable,
        "filter_method": filter_method,
        "cpu_source": cpu_source,
    }


def find_probes(sweep_dir):
    """Find all producers-N probe directories under sweep_dir."""
    probes = []
    for root, dirs, _ in os.walk(sweep_dir):
        for d in sorted(dirs):
            m = re.match(r"^producers-(\d+)$", d)
            if m:
                probes.append((int(m.group(1)), os.path.join(root, d)))
    probes.sort(key=lambda x: x[0])
    return probes


def main():
    parser = argparse.ArgumentParser(
        description="Derive proxy CPU sizing coefficient from connection-sweep results"
    )
    parser.add_argument("sweep_dir", help="Root of a connection-sweep output directory")
    parser.add_argument(
        "--skip-first",
        type=int,
        default=10,
        metavar="N",
        help="Fallback: skip first N CPU snapshots when timestamps absent (default: 10)",
    )
    args = parser.parse_args()

    probes = find_probes(args.sweep_dir)
    if not probes:
        print(f"No producers-N directories found under {args.sweep_dir}", file=sys.stderr)
        sys.exit(1)

    print(f"{'Producers':>9}  {'Achieved':>10}  {'Target':>10}  {'Sat':>4}  "
          f"{'CPU (mc)':>9}  {'Snapshots':>11}  {'Filter':>10}  {'Source':>14}  {'Coeff (mc/MB/s)':>16}")
    print("-" * 110)

    coefficients = []
    last_data = None

    for n, probe_dir in probes:
        data = load_probe(probe_dir, args.skip_first)
        if data is None:
            print(f"{n:>9}  (no data)")
            continue

        achieved = data["achieved_rate"] * data.get("topics", 1)
        msg_size = data["message_size"]
        usable = data["usable_snapshots"]
        target = data["target"]
        saturated = target is not None and achieved < 0.95 * target

        cpu_avg = statistics.mean(usable) if usable else float("nan")
        cpu_millicores = cpu_avg * 1000

        total_mb_per_s = (achieved * msg_size * 2) / 1_000_000
        coeff = cpu_millicores / total_mb_per_s if total_mb_per_s > 0 else float("nan")

        sat_flag = "*" if saturated else " "
        target_str = f"{target:>10,.0f}" if target else f"{'?':>10}"
        snap_str = f"{len(usable)}/{data['all_snapshots']}"
        print(f"{n:>9}  {achieved:>10,.0f}  {target_str}  {sat_flag:>4}  "
              f"{cpu_millicores:>9.0f}  {snap_str:>11}  {data['filter_method']:>10}  "
              f"{data.get('cpu_source','?'):>14}  {coeff:>16.1f}")

        if not saturated:
            coefficients.append(coeff)
        last_data = data

    print()
    if coefficients:
        mean_coeff = statistics.mean(coefficients)
        stdev_coeff = statistics.stdev(coefficients) if len(coefficients) > 1 else 0.0

        print(f"Sizing coefficient (non-saturated probes only): {mean_coeff:.1f} mc/MB/s  "
              f"(±{stdev_coeff:.1f} stdev, n={len(coefficients)})")
        print()
        print("Sizing formula:")
        print(f"  proxy_CPU_millicores = {mean_coeff:.0f} × (produce_MB_per_s + consume_MB_per_s)")
        print()
        print("Notes:")
        print("  - Assumes encrypt ≈ decrypt CPU cost (bidirectional measurement)")
        print("  - Add headroom (e.g. ×1.3) for burst and GC pauses")
        if last_data:
            print(f"  - Coefficient measured at {last_data['message_size']} bytes/message")
        print("  - (* = saturated probe, excluded from coefficient)")
    else:
        print("No non-saturated probes found — cannot compute coefficient.")
        print("Run the sweep at a lower per-producer rate or with fewer steps.")


if __name__ == "__main__":
    main()
