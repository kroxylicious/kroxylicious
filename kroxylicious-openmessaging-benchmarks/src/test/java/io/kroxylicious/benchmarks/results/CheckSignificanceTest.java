/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.benchmarks.results;

import java.net.URL;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class CheckSignificanceTest {

    private static String path(String resource) {
        URL url = CheckSignificanceTest.class.getResource("/" + resource);
        assertThat(url).as("test resource %s", resource).isNotNull();
        return url.getPath();
    }

    @Test
    void exitZeroWhenE2eP99DeltaIsSignificant() {
        int exit = CheckSignificance.execute(path("omb-result-baseline.json"), path("omb-result-proxy.json"));
        assertThat(exit).as("should exit 0 (significant)").isEqualTo(0);
    }

    @Test
    void exitOneWhenBaselineAndCandidateAreIdentical() {
        String p = path("omb-result-baseline.json");
        int exit = CheckSignificance.execute(p, p);
        assertThat(exit).as("should exit 1 (not significant — identical inputs)").isEqualTo(1);
    }
}
