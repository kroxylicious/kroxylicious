/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.benchmarks.results;

import java.io.PrintStream;
import java.util.List;
import java.util.Optional;

/**
 * Compares two {@link OmbResult} instances and outputs a formatted table
 * showing latency and throughput metrics side-by-side with deltas.
 */
public class ResultComparator {

    private static final String SEPARATOR = "------------";

    private final OmbResult baseline;
    private final OmbResult candidate;
    private final SignificanceTester significanceTester;

    public ResultComparator(OmbResult baseline, OmbResult candidate) {
        this(baseline, candidate, new SignificanceTester());
    }

    ResultComparator(OmbResult baseline, OmbResult candidate, SignificanceTester significanceTester) {
        this.baseline = baseline;
        this.candidate = candidate;
        this.significanceTester = significanceTester;
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
        List<LatencyComparison> rows = List.of(
                new LatencyComparison("Avg", baseline.getPublishLatencyAvg(), candidate.getPublishLatencyAvg(), null, null),
                new LatencyComparison("p50", baseline.getPublishLatency50pct(), candidate.getPublishLatency50pct(), null, null),
                new LatencyComparison("p95", baseline.getPublishLatency95pct(), candidate.getPublishLatency95pct(), null, null),
                new LatencyComparison("p99", baseline.getPublishLatency99pct(), candidate.getPublishLatency99pct(),
                        baseline.getPublishLatency99pctWindows(), candidate.getPublishLatency99pctWindows()),
                new LatencyComparison("p99.9", baseline.getPublishLatency999pct(), candidate.getPublishLatency999pct(), null, null));
        printSection(out, "Publish Latency (ms)", rows);
    }

    private void printEndToEndLatency(PrintStream out) {
        List<LatencyComparison> rows = List.of(
                new LatencyComparison("Avg", baseline.getAggregatedEndToEndLatencyAvg(), candidate.getAggregatedEndToEndLatencyAvg(), null, null),
                new LatencyComparison("p50", baseline.getAggregatedEndToEndLatency50pct(), candidate.getAggregatedEndToEndLatency50pct(), null, null),
                new LatencyComparison("p95", baseline.getAggregatedEndToEndLatency95pct(), candidate.getAggregatedEndToEndLatency95pct(), null, null),
                new LatencyComparison("p99", baseline.getAggregatedEndToEndLatency99pct(), candidate.getAggregatedEndToEndLatency99pct(),
                        baseline.getEndToEndLatency99pctWindows(), candidate.getEndToEndLatency99pctWindows()),
                new LatencyComparison("p99.9", baseline.getAggregatedEndToEndLatency999pct(), candidate.getAggregatedEndToEndLatency999pct(), null, null));
        printSection(out, "End-to-End Latency (ms)", rows);
    }

    private void printThroughput(PrintStream out) {
        printSectionHeader(out, "Total Throughput (msg/s)");
        printRow(out, "Publish Rate", baseline.getPublishRate(), candidate.getPublishRate());
        printRow(out, "Consume Rate", baseline.getConsumeRate(), candidate.getConsumeRate());
    }

    private void printSection(PrintStream out, String title, List<LatencyComparison> rows) {
        printSectionHeader(out, title);
        for (LatencyComparison row : rows) {
            Optional<SignificanceTester.Result> sig = row.assess(significanceTester);
            printLatencyRow(out, row, sig.orElse(null));
        }
    }

    private static void printSectionHeader(PrintStream out, String title) {
        out.println();
        out.println(title);
        out.printf("  %-25s %12s %12s %12s%n", "Metric", "Baseline", "Candidate", "Delta");
        out.printf("  %-25s %12s %12s %12s%n",
                "-------------------------", SEPARATOR, SEPARATOR, SEPARATOR);
    }

    private static void printLatencyRow(PrintStream out, LatencyComparison c, SignificanceTester.Result sig) {
        String pctSign = c.pct() > 0 ? "+" : "";
        String sigSuffix = sig == null ? ""
                : sig.significant()
                        ? String.format("  p=%.4f *", sig.pValue())
                        : String.format("  p=%.4f", sig.pValue());
        out.printf("  %-25s %12.2f %12.2f %12.2f (%s%.1f%%)%s%n",
                c.label(), c.baseline(), c.candidate(), c.delta(), pctSign, c.pct(), sigSuffix);
    }

    private static void printRow(PrintStream out, String label, double baselineVal, double candidateVal) {
        double delta = candidateVal - baselineVal;
        double pct = baselineVal != 0 ? delta / baselineVal * 100.0 : 0.0;
        String pctSign = pct > 0 ? "+" : "";
        out.printf("  %-25s %12.2f %12.2f %12.2f (%s%.1f%%)%n",
                label, baselineVal, candidateVal, delta, pctSign, pct);
    }
}
