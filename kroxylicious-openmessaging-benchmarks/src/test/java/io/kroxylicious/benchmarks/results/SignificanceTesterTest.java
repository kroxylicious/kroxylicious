/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.benchmarks.results;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests our threshold logic and integration plumbing — not Commons Math's correctness.
 */
class SignificanceTesterTest {

    private static final double[] DUMMY = { 1.0 };

    @Test
    void significantWhenPValueBelowThreshold() {
        var tester = new SignificanceTester((a, b) -> 0.04);
        var result = tester.test(DUMMY, DUMMY);
        assertThat(result.significant()).isTrue();
        assertThat(result.pValue()).isEqualTo(0.04);
    }

    @Test
    void notSignificantWhenPValueAtThreshold() {
        var tester = new SignificanceTester((a, b) -> 0.05);
        var result = tester.test(DUMMY, DUMMY);
        assertThat(result.significant()).isFalse();
    }

    @Test
    void notSignificantWhenPValueAboveThreshold() {
        var tester = new SignificanceTester((a, b) -> 0.99);
        var result = tester.test(DUMMY, DUMMY);
        assertThat(result.significant()).isFalse();
    }

    @Test
    void pValueIsPassedThroughToResult() {
        var tester = new SignificanceTester((a, b) -> 0.123);
        assertThat(tester.test(DUMMY, DUMMY).pValue()).isEqualTo(0.123);
    }
}
