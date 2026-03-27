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
    private static final String SEPARATOR_NARROW = "----------";

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
        out.println();
        out.println("  * p < 0.05 (Mann-Whitney U on per-window samples; samples are aggregated window statistics, not individual message latencies)");
    }

    private void printPublishLatency(PrintStream out) {
        List<LatencyComparison> rows = List.of(
                new LatencyComparison("Avg", baseline.getPublishLatencyAvg(), candidate.getPublishLatencyAvg(),
                        baseline.getPublishLatencyAvgWindows(), candidate.getPublishLatencyAvgWindows()),
                new LatencyComparison("p50", baseline.getPublishLatency50pct(), candidate.getPublishLatency50pct(),
                        baseline.getPublishLatency50pctWindows(), candidate.getPublishLatency50pctWindows()),
                new LatencyComparison("p95", baseline.getPublishLatency95pct(), candidate.getPublishLatency95pct(),
                        baseline.getPublishLatency95pctWindows(), candidate.getPublishLatency95pctWindows()),
                new LatencyComparison("p99", baseline.getPublishLatency99pct(), candidate.getPublishLatency99pct(),
                        baseline.getPublishLatency99pctWindows(), candidate.getPublishLatency99pctWindows()),
                new LatencyComparison("p99.9", baseline.getPublishLatency999pct(), candidate.getPublishLatency999pct(),
                        baseline.getPublishLatency999pctWindows(), candidate.getPublishLatency999pctWindows()));
        printLatencySection(out, "Publish Latency (ms)", rows);
    }

    private void printEndToEndLatency(PrintStream out) {
        List<LatencyComparison> rows = List.of(
                new LatencyComparison("Avg", baseline.getAggregatedEndToEndLatencyAvg(), candidate.getAggregatedEndToEndLatencyAvg(),
                        baseline.getEndToEndLatencyAvgWindows(), candidate.getEndToEndLatencyAvgWindows()),
                new LatencyComparison("p50", baseline.getAggregatedEndToEndLatency50pct(), candidate.getAggregatedEndToEndLatency50pct(),
                        baseline.getEndToEndLatency50pctWindows(), candidate.getEndToEndLatency50pctWindows()),
                new LatencyComparison("p95", baseline.getAggregatedEndToEndLatency95pct(), candidate.getAggregatedEndToEndLatency95pct(),
                        baseline.getEndToEndLatency95pctWindows(), candidate.getEndToEndLatency95pctWindows()),
                new LatencyComparison("p99", baseline.getAggregatedEndToEndLatency99pct(), candidate.getAggregatedEndToEndLatency99pct(),
                        baseline.getEndToEndLatency99pctWindows(), candidate.getEndToEndLatency99pctWindows()),
                new LatencyComparison("p99.9", baseline.getAggregatedEndToEndLatency999pct(), candidate.getAggregatedEndToEndLatency999pct(),
                        baseline.getEndToEndLatency999pctWindows(), candidate.getEndToEndLatency999pctWindows()));
        printLatencySection(out, "End-to-End Latency (ms)", rows);
    }

    private void printThroughput(PrintStream out) {
        out.println();
        out.println("Total Throughput (msg/s)");
        out.printf("  %-25s %12s %12s %12s %10s%n", "Metric", "Baseline", "Candidate", "Delta", "%Change");
        out.printf("  %-25s %12s %12s %12s %10s%n",
                "-------------------------", SEPARATOR, SEPARATOR, SEPARATOR, SEPARATOR_NARROW);
        printRow(out, "Publish Rate", baseline.getPublishRate(), candidate.getPublishRate());
        printRow(out, "Consume Rate", baseline.getConsumeRate(), candidate.getConsumeRate());
    }

    private void printLatencySection(PrintStream out, String title, List<LatencyComparison> rows) {
        out.println();
        out.println(title);
        out.printf("  %-25s %12s %12s %12s %10s %10s%n", "Metric", "Baseline", "Candidate", "Delta", "%Change", "MWU p");
        out.printf("  %-25s %12s %12s %12s %10s %10s%n",
                "-------------------------", SEPARATOR, SEPARATOR, SEPARATOR, SEPARATOR_NARROW, SEPARATOR_NARROW);
        for (LatencyComparison row : rows) {
            Optional<SignificanceTester.Result> sig = row.assess(significanceTester);
            printLatencyRow(out, row, sig.orElse(null));
        }
    }

    private static void printLatencyRow(PrintStream out, LatencyComparison c, SignificanceTester.Result sig) {
        String pctChange = String.format("%+.1f%%", c.pct());
        String pValue = sig == null ? "         " : String.format("%9.4f%s", sig.pValue(), sig.significant() ? "*" : " ");
        out.printf("  %-25s %12.2f %12.2f %12.2f %10s %10s%n",
                c.label(), c.baseline(), c.candidate(), c.delta(), pctChange, pValue);
    }

    private static void printRow(PrintStream out, String label, double baselineVal, double candidateVal) {
        double delta = candidateVal - baselineVal;
        double pct = baselineVal != 0 ? delta / baselineVal * 100.0 : 0.0;
        out.printf("  %-25s %12.2f %12.2f %12.2f %+10.1f%%%n",
                label, baselineVal, candidateVal, delta, pct);
    }
}
