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
//DEPS org.apache.commons:commons-math3:3.6.1
//SOURCES OmbResult.java
//SOURCES LatencyComparison.java
//SOURCES SignificanceTester.java
import java.io.File;
import java.io.IOException;
import java.util.concurrent.Callable;

import com.fasterxml.jackson.databind.ObjectMapper;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

/**
 * CLI tool that exits 0 if the end-to-end p99 latency delta between two OMB result files
 * is statistically significant (MWU p &lt; 0.05), or 1 if it is not.
 * <p>
 * Intended for use in shell scripts to annotate sweep summary tables with noise warnings.
 */
@Command(name = "check-significance", mixinStandardHelpOptions = true, description = "Exits 0 if the e2e-p99 delta between two OMB result files is statistically significant (MWU p < 0.05), or 1 if not.")
@SuppressWarnings({ "checkstyle:RegexpSinglelineJava", "java:S106" }) // CLI tool that intentionally writes to System.err
public class CheckSignificance implements Callable<Integer> {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Parameters(index = "0", description = "Path to the baseline OMB result JSON file")
    private File baselineFile;

    @Parameters(index = "1", description = "Path to the candidate OMB result JSON file")
    private File candidateFile;

    public static void main(String... args) {
        System.exit(execute(args));
    }

    static int execute(String... args) {
        return new CommandLine(new CheckSignificance()).execute(args);
    }

    @Override
    public Integer call() throws IOException {
        OmbResult baseline = MAPPER.readValue(baselineFile, OmbResult.class);
        OmbResult candidate = MAPPER.readValue(candidateFile, OmbResult.class);
        LatencyComparison p99 = new LatencyComparison("p99",
                baseline.getAggregatedEndToEndLatency99pct(), candidate.getAggregatedEndToEndLatency99pct(),
                baseline.getEndToEndLatency99pctWindows(), candidate.getEndToEndLatency99pctWindows());
        return p99.assess(new SignificanceTester())
                .map(r -> r.significant() ? 0 : 1)
                .orElse(1);
    }
}
