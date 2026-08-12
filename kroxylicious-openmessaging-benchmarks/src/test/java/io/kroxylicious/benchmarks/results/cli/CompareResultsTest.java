/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.benchmarks.results.cli;

import java.net.URL;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class CompareResultsTest {

    private static String path(String resource) {
        URL url = CompareResultsTest.class.getResource("/" + resource);
        assertThat(url).as("test resource %s", resource).isNotNull();
        return url.getPath();
    }

    @Test
    void exitZeroWhenComparingTwoResultFiles() {
        // When
        int exit = CompareResults.execute(path("omb-result-baseline.json"), path("omb-result-proxy.json"));

        // Then
        assertThat(exit).isZero();
    }

    @Test
    void exitNonZeroWhenResultFileIsMissing() {
        // When
        int exit = CompareResults.execute(path("omb-result-baseline.json"), "does-not-exist.json");

        // Then
        assertThat(exit).isNotZero();
    }
}
