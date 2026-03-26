/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.benchmarks.results;

///usr/bin/env jbang "$0" "$@" ; exit $?
//JAVA 21+
//DEPS com.fasterxml.jackson.core:jackson-core:${jackson.version}
//DEPS com.fasterxml.jackson.core:jackson-databind:${jackson.version}
//DEPS info.picocli:picocli:${picocli.version}
//SOURCES OmbResult.java
//SOURCES BackPressureAnalyser.java
//SOURCES RunMetadataResult.java
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.kroxylicious.benchmarks.results.BackPressureAnalyser.LabelledResult;
import io.kroxylicious.benchmarks.results.BackPressureAnalyser.Report;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

/**
 * CLI tool for detecting producer back-pressure in OpenMessaging Benchmark result files.
 * <p>
 * Exits 0 if no back-pressure detected, 1 if any result shows back-pressure — making it
 * composable in scripts and from {@code run-all-scenarios.sh}.
 */
@Command(name = "check-backpressure", mixinStandardHelpOptions = true, description = "Check OMB result files for producer back-pressure indicating broker saturation.")
@SuppressWarnings({ "checkstyle:RegexpSinglelineJava", "java:S106" })
public class CheckBackPressure implements Callable<Integer> {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Parameters(description = "OMB result JSON files to check")
    private List<File> resultFiles;

    @Option(names = "--workload", description = "Workload name to include in the rate-sweep hint")
    private String workload;

    public static void main(String... args) {
        System.exit(execute(args));
    }

    static int execute(String... args) {
        return new CommandLine(new CheckBackPressure()).execute(args);
    }

    @Override
    public Integer call() throws IOException {
        List<LabelledResult> labelled = resultFiles.stream()
                .map(f -> {
                    RunMetadataResult meta = RunMetadataResult.fromSiblingOf(f).orElse(null);
                    String label = meta != null ? meta.scenario() : fallbackLabel(f);
                    return new LabelledResult(label, readQuietly(f));
                })
                .toList();

        List<Report> saturated = new BackPressureAnalyser().analyse(labelled);

        for (LabelledResult lr : labelled) {
            double delayMs = lr.result().getPublishDelayLatencyAvgNs() / 1_000_000.0;
            double p99Ms = lr.result().getPublishDelayLatency99pctNs() / 1_000_000.0;
            boolean sat = saturated.stream().anyMatch(r -> r.label().equals(lr.label()));
            if (sat) {
                System.out.printf("  %-30s  delay avg %8.1f ms  p99 %8.1f ms  *** BACK-PRESSURE ***%n",
                        lr.label(), delayMs, p99Ms);
            }
            else {
                System.out.printf("  %-30s  delay avg %8.4f ms  p99 %8.4f ms%n",
                        lr.label(), delayMs, p99Ms);
            }
        }

        if (!saturated.isEmpty()) {
            String resolvedWorkload = workload != null ? workload : inferWorkload();
            printWarning(saturated, resolvedWorkload);
            return 1;
        }
        return 0;
    }

    private void printWarning(List<Report> saturated, String resolvedWorkload) {
        System.out.println();
        System.out.println("WARNING: Producer back-pressure detected in " + saturated.size()
                + " of " + resultFiles.size() + " result(s).");
        System.out.println("Producers were blocking on full send buffers — single-partition saturation");
        System.out.println("at the broker is masking overhead measurements.");
        System.out.println();
        System.out.println("Consider a rate sweep to find the saturation knee:");
        Report first = saturated.get(0);
        System.out.println("  scripts/rate-sweep.sh \\");
        if (resolvedWorkload != null) {
            System.out.println("    --workload " + resolvedWorkload + " \\");
        }
        System.out.println("    --min-rate " + first.suggestedMinRate() + " --max-rate " + first.suggestedMaxRate() + " \\");
        System.out.println("    --output-dir ./results/sweep-$(date +%Y%m%d-%H%M%S)/");
    }

    /** Reads the workload from any sibling metadata file, returning null if none found. */
    private String inferWorkload() {
        return resultFiles.stream()
                .map(RunMetadataResult::fromSiblingOf)
                .filter(Optional::isPresent)
                .map(opt -> opt.get().workload())
                .filter(w -> w != null && !w.isBlank())
                .findFirst()
                .orElse(null);
    }

    private static String fallbackLabel(File f) {
        return f.getParentFile() != null ? f.getParentFile().getName() : f.getName();
    }

    private static OmbResult readQuietly(File f) {
        try {
            return MAPPER.readValue(f, OmbResult.class);
        }
        catch (IOException e) {
            throw new IllegalArgumentException("Cannot read result file: " + f, e);
        }
    }
}
