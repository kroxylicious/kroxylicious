/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.benchmarks.results;

import java.util.Optional;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class LatencyComparisonTest {

    @Test
    void deltaIsCandidateMinusBaseline() {
        var c = new LatencyComparison("p99", 25.0, 29.0, null, null);
        assertThat(c.delta()).isEqualTo(4.0);
    }

    @Test
    void deltaIsNegativeWhenCandidateIsLower() {
        var c = new LatencyComparison("p99", 29.0, 25.0, null, null);
        assertThat(c.delta()).isEqualTo(-4.0);
    }

    @Test
    void pctIsRelativeDelta() {
        var c = new LatencyComparison("p99", 25.0, 30.0, null, null);
        assertThat(c.pct()).isEqualTo(20.0);
    }

    @Test
    void pctIsZeroWhenBaselineIsZero() {
        var c = new LatencyComparison("avg", 0.0, 5.0, null, null);
        assertThat(c.pct()).isEqualTo(0.0);
    }

    @Test
    void assessReturnsEmptyWhenWindowsAreNull() {
        var c = new LatencyComparison("p99", 25.0, 29.0, null, null);
        assertThat(c.assess(new SignificanceTester())).isEmpty();
    }

    @Test
    void assessReturnsPValueFromTester() {
        double[] windows = { 1.0, 2.0, 3.0 };
        var tester = new SignificanceTester((a, b) -> 0.03);
        var c = new LatencyComparison("p99", 25.0, 29.0, windows, windows);
        Optional<SignificanceTester.Result> result = c.assess(tester);
        assertThat(result).isPresent().get().satisfies(r -> {
            assertThat(r.pValue()).isEqualTo(0.03);
            assertThat(r.significant()).isTrue();
        });
    }
}
