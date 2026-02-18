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
import java.io.File;
import java.io.IOException;
import java.util.concurrent.Callable;

import com.fasterxml.jackson.databind.ObjectMapper;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

/**
 * CLI tool for comparing two OpenMessaging Benchmark result files.
 * <p>
 * Designed to be run via JBang with Maven resource filtering to resolve
 * dependency versions from the parent pom.
 */
@Command(name = "compare-results", mixinStandardHelpOptions = true, description = "Compare two OpenMessaging Benchmark result files and display a table showing latency and throughput metrics side-by-side with deltas.")
public class CompareResults implements Callable<Integer> {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Parameters(index = "0", description = "Path to the baseline OMB result JSON file")
    private File baselineFile;

    @Parameters(index = "1", description = "Path to the candidate OMB result JSON file")
    private File candidateFile;

    public static void main(String... args) {
        int exitCode = execute(args);
        System.exit(exitCode);
    }

    static int execute(String... args) {
        return new CommandLine(new CompareResults()).execute(args);
    }

    @Override
    public Integer call() throws IOException {
        OmbResult baseline = MAPPER.readValue(baselineFile, OmbResult.class);
        OmbResult candidate = MAPPER.readValue(candidateFile, OmbResult.class);
        new ResultComparator(baseline, candidate).compare(System.out);
        return 0;
    }
}
