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
//DEPS org.apache.commons:commons-math3:3.6.1
//SOURCES OmbResult.java
//SOURCES LatencyComparison.java
//SOURCES SignificanceTester.java
import java.io.File;
import java.io.IOException;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * CLI tool that exits 0 if the end-to-end p99 latency delta between two OMB result files
 * is statistically significant (MWU p &lt; 0.05), or 1 if it is not.
 * <p>
 * Intended for use in shell scripts to annotate sweep summary tables with noise warnings.
 */
// TODO: update sweep summary saturation indicator to use [N] footnote callouts instead of sat@N(-D) syntax
// TODO: update ResultComparator to use [N] footnote callouts instead of * markers, flipping to assume significance
public class CheckSignificance {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String... args) {
        System.exit(execute(args[0], args[1]));
    }

    static int execute(String baselinePath, String candidatePath) {
        try {
            OmbResult baseline = MAPPER.readValue(new File(baselinePath), OmbResult.class);
            OmbResult candidate = MAPPER.readValue(new File(candidatePath), OmbResult.class);
            LatencyComparison p99 = new LatencyComparison("p99",
                    baseline.getAggregatedEndToEndLatency99pct(), candidate.getAggregatedEndToEndLatency99pct(),
                    baseline.getEndToEndLatency99pctWindows(), candidate.getEndToEndLatency99pctWindows());
            return p99.assess(new SignificanceTester())
                    .map(r -> r.significant() ? 0 : 1)
                    .orElse(1);
        }
        catch (IOException e) {
            System.err.println("Error reading result files: " + e.getMessage());
            return 2;
        }
    }
}
