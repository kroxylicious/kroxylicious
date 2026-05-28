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
    python3 analyze-cpu-coefficient.py --compare <dir1> [<dir2> ...] [--skip-first N]

    sweep-output-dir  Root of a connection-sweep run, e.g. /tmp/results/conn-sweep/
    --compare         Print a one-row-per-config summary table across multiple directories.
                      Each dir may be prefixed with a label: "label:path"
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


def analyze_sweep(sweep_dir, skip_first):
    """Analyze a sweep directory; return (probe_rows, summary_stats, last_data).

    probe_rows is a list of dicts for the per-probe table.
    summary_stats is a dict with mean, stdev, n, sat, or None if no usable data.
    """
    probes = find_probes(sweep_dir)
    rows = []
    coefficients = []
    sat_count = 0
    last_data = None

    for n, probe_dir in probes:
        data = load_probe(probe_dir, skip_first)
        if data is None:
            rows.append({"producers": n, "no_data": True})
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

        rows.append({
            "producers": n,
            "achieved": achieved,
            "target": target,
            "saturated": saturated,
            "cpu_millicores": cpu_millicores,
            "usable_count": len(usable),
            "all_count": data["all_snapshots"],
            "filter_method": data["filter_method"],
            "cpu_source": data.get("cpu_source", "?"),
            "coeff": coeff,
        })

        if saturated:
            sat_count += 1
        else:
            coefficients.append(coeff)
        last_data = data

    if not coefficients:
        return rows, None, last_data

    mean_coeff = statistics.mean(coefficients)
    stdev_coeff = statistics.stdev(coefficients) if len(coefficients) > 1 else 0.0
    summary = {
        "mean": mean_coeff,
        "stdev": stdev_coeff,
        "n": len(coefficients),
        "sat": sat_count,
    }
    return rows, summary, last_data


def print_full_table(sweep_dir, skip_first):
    """Print the detailed per-probe table for a single sweep directory."""
    rows, summary, last_data = analyze_sweep(sweep_dir, skip_first)

    print(f"{'Producers':>9}  {'Achieved':>10}  {'Target':>10}  {'Sat':>4}  "
          f"{'CPU (mc)':>9}  {'Snapshots':>11}  {'Filter':>10}  {'Source':>14}  {'Coeff (mc/MB/s)':>16}")
    print("-" * 110)

    for row in rows:
        if row.get("no_data"):
            print(f"{row['producers']:>9}  (no data)")
            continue
        sat_flag = "*" if row["saturated"] else " "
        target_str = f"{row['target']:>10,.0f}" if row["target"] else f"{'?':>10}"
        snap_str = f"{row['usable_count']}/{row['all_count']}"
        print(f"{row['producers']:>9}  {row['achieved']:>10,.0f}  {target_str}  {sat_flag:>4}  "
              f"{row['cpu_millicores']:>9.0f}  {snap_str:>11}  {row['filter_method']:>10}  "
              f"{row['cpu_source']:>14}  {row['coeff']:>16.1f}")

    print()
    if summary:
        print(f"Sizing coefficient (non-saturated probes only): {summary['mean']:.1f} mc/MB/s  "
              f"(±{summary['stdev']:.1f} stdev, n={summary['n']})")
        print()
        print("Sizing formula:")
        print(f"  proxy_CPU_millicores = {summary['mean']:.0f} × (produce_MB_per_s + consume_MB_per_s)")
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


def derive_label(path):
    """Derive a short display label from a sweep directory path."""
    parent = os.path.basename(os.path.dirname(os.path.abspath(path)))
    label = re.sub(r"^conn-sweep-[^-]+-", "", parent)
    label = re.sub(r"-rf\d+$", "", label)
    return label or parent


def parse_compare_arg(arg):
    """Parse 'label:path' or plain 'path'; return (label, path)."""
    if ":" in arg:
        label, _, path = arg.partition(":")
        return label.strip(), path.strip()
    return derive_label(arg), arg


def print_comparison_table(compare_args, skip_first):
    """Print a one-row-per-config summary table."""
    entries = [parse_compare_arg(a) for a in compare_args]

    col_label = max(len(label) for label, _ in entries)
    col_label = max(col_label, 6)

    header = (f"{'Config':<{col_label}}  {'Coeff (mc/MB/s)':>16}  "
              f"{'Stdev':>7}  {'n':>3}  {'Sat':>4}  Formula")
    print(header)
    print("-" * len(header))

    for label, path in entries:
        _, summary, _ = analyze_sweep(path, skip_first)
        if summary is None:
            print(f"{label:<{col_label}}  (no usable data)")
            continue
        sat_str = f"{summary['sat']}*" if summary["sat"] else "  -"
        formula = f"{summary['mean']:.0f} × MB/s"
        print(f"{label:<{col_label}}  {summary['mean']:>13.1f} mc/MB/s  "
              f"±{summary['stdev']:>5.1f}  {summary['n']:>3}  {sat_str:>4}  {formula}")


def main():
    parser = argparse.ArgumentParser(
        description="Derive proxy CPU sizing coefficient from connection-sweep results"
    )
    parser.add_argument(
        "sweep_dir",
        nargs="?",
        help="Root of a connection-sweep output directory (single-run mode)",
    )
    parser.add_argument(
        "--compare",
        nargs="+",
        metavar="DIR",
        help="Compare multiple sweep directories (one row each). Each entry may be 'label:path'.",
    )
    parser.add_argument(
        "--skip-first",
        type=int,
        default=10,
        metavar="N",
        help="Fallback: skip first N CPU snapshots when timestamps absent (default: 10)",
    )
    args = parser.parse_args()

    if args.compare and args.sweep_dir:
        parser.error("Specify either a sweep_dir or --compare, not both.")
    if not args.compare and not args.sweep_dir:
        parser.error("Specify a sweep_dir or use --compare.")

    if args.compare:
        print_comparison_table(args.compare, args.skip_first)
    else:
        print_full_table(args.sweep_dir, args.skip_first)


if __name__ == "__main__":
    main()
