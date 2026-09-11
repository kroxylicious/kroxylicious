/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.benchmarks.results;

import java.util.Arrays;
import java.util.Optional;

/**
 * Holds pre-computed comparison values for a single latency metric between baseline and candidate.
 * <p>
 * Rendering is separated from computation: callers build a {@code LatencyComparison} first,
 * then pass it to a render method. Significance is computed on demand via {@link #assess}.
 *
 * @param label            display name for the metric being compared (e.g. {@code p99})
 * @param baseline         full-run aggregated value for the baseline
 * @param candidate        full-run aggregated value for the candidate
 * @param baselineWindows  per-sample-window values for the baseline, or {@code null} if not available
 * @param candidateWindows per-sample-window values for the candidate, or {@code null} if not available
 */
public record LatencyComparison(String label, double baseline, double candidate,
                                @SuppressWarnings("ArrayRecordComponent") double[] baselineWindows, @SuppressWarnings("ArrayRecordComponent") double[] candidateWindows) { // double[] retained: deep
                                                                                                                                                                           // equality via explicit
                                                                                                                                                                           // equals/hashCode below

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof LatencyComparison other)) {
            return false;
        }
        return Double.compare(baseline, other.baseline) == 0
                && Double.compare(candidate, other.candidate) == 0
                && label.equals(other.label)
                && Arrays.equals(baselineWindows, other.baselineWindows)
                && Arrays.equals(candidateWindows, other.candidateWindows);
    }

    @Override
    public int hashCode() {
        int result = label.hashCode();
        result = 31 * result + Double.hashCode(baseline);
        result = 31 * result + Double.hashCode(candidate);
        result = 31 * result + Arrays.hashCode(baselineWindows);
        result = 31 * result + Arrays.hashCode(candidateWindows);
        return result;
    }

    @Override
    public String toString() {
        return "LatencyComparison[label=" + label
                + ", baseline=" + baseline
                + ", candidate=" + candidate
                + ", baselineWindows=" + Arrays.toString(baselineWindows)
                + ", candidateWindows=" + Arrays.toString(candidateWindows) + "]";
    }

    /**
     * The absolute difference between candidate and baseline.
     *
     * @return candidate value minus baseline value
     */
    public double delta() {
        return candidate - baseline;
    }

    /**
     * The relative difference between candidate and baseline.
     *
     * @return percentage change from baseline to candidate, or {@code 0.0} if the baseline is zero
     */
    public double pct() {
        return baseline != 0 ? delta() / baseline * 100.0 : 0.0;
    }

    /**
     * Tests whether the difference between the per-window samples is statistically significant.
     *
     * @param tester the significance tester to apply
     * @return the test result, or empty if either per-window sample array is absent
     */
    public Optional<SignificanceTester.Result> assess(SignificanceTester tester) {
        if (baselineWindows == null || candidateWindows == null) {
            return Optional.empty();
        }
        return Optional.of(tester.test(baselineWindows, candidateWindows));
    }
}
