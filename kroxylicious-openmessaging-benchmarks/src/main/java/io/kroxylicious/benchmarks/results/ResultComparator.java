/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.benchmarks.results;

import java.io.PrintStream;

/**
 * Compares two {@link OmbResult} instances and outputs a formatted table
 * showing latency and throughput metrics side-by-side with deltas.
 */
public class ResultComparator {

    private final OmbResult baseline;
    private final OmbResult candidate;

    public ResultComparator(OmbResult baseline, OmbResult candidate) {
        this.baseline = baseline;
        this.candidate = candidate;
    }

    /**
     * Writes the comparison table to the given output stream.
     *
     * @param out the stream to write to
     */
    public void compare(PrintStream out) {
        printPublishLatency(out);
        printEndToEndLatency(out);
        printThroughput(out);
    }

    private void printPublishLatency(PrintStream out) {
        printSectionHeader(out, "Publish Latency (ms)");
        printRow(out, "Avg", baseline.getPublishLatencyAvg(), candidate.getPublishLatencyAvg());
        printRow(out, "p50", baseline.getPublishLatency50pct(), candidate.getPublishLatency50pct());
        printRow(out, "p95", baseline.getPublishLatency95pct(), candidate.getPublishLatency95pct());
        printRow(out, "p99", baseline.getPublishLatency99pct(), candidate.getPublishLatency99pct());
        printRow(out, "p99.9", baseline.getPublishLatency999pct(), candidate.getPublishLatency999pct());
    }

    private void printEndToEndLatency(PrintStream out) {
        printSectionHeader(out, "End-to-End Latency (ms)");
        printRow(out, "Avg", baseline.getEndToEndLatencyAvgMean(), candidate.getEndToEndLatencyAvgMean());
        printRow(out, "p50", baseline.getEndToEndLatency50pctMean(), candidate.getEndToEndLatency50pctMean());
        printRow(out, "p95", baseline.getEndToEndLatency95pctMean(), candidate.getEndToEndLatency95pctMean());
        printRow(out, "p99", baseline.getEndToEndLatency99pctMean(), candidate.getEndToEndLatency99pctMean());
        printRow(out, "p99.9", baseline.getEndToEndLatency999pctMean(), candidate.getEndToEndLatency999pctMean());
    }

    private void printThroughput(PrintStream out) {
        printSectionHeader(out, "Throughput (msg/s)");
        printRow(out, "Publish Rate", baseline.getPublishRateMean(), candidate.getPublishRateMean());
        printRow(out, "Consume Rate", baseline.getConsumeRateMean(), candidate.getConsumeRateMean());
    }

    private static void printSectionHeader(PrintStream out, String title) {
        out.println();
        out.println(title);
        out.printf("  %-25s %12s %12s %12s%n", "Metric", "Baseline", "Candidate", "Delta");
        out.printf("  %-25s %12s %12s %12s%n",
                "-------------------------", "------------", "------------", "------------");
    }

    private static void printRow(PrintStream out, String label, double baselineVal, double candidateVal) {
        double delta = candidateVal - baselineVal;
        double pct = baselineVal != 0 ? delta / baselineVal * 100.0 : 0.0;
        String pctSign = pct > 0 ? "+" : "";
        out.printf("  %-25s %12.2f %12.2f %12.2f (%s%.1f%%)%n",
                label, baselineVal, candidateVal, delta, pctSign, pct);
    }
}
