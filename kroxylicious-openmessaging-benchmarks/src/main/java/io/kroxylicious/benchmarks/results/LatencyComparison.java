/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.benchmarks.results;

import java.util.Optional;

/**
 * Holds pre-computed comparison values for a single latency metric between baseline and candidate.
 * <p>
 * Rendering is separated from computation: callers build a {@code LatencyComparison} first,
 * then pass it to a render method. Significance is computed on demand via {@link #assess}.
 */
record LatencyComparison(String label, double baseline, double candidate,
                         double[] baselineWindows, double[] candidateWindows) {

    double delta() {
        return candidate - baseline;
    }

    double pct() {
        return baseline != 0 ? delta() / baseline * 100.0 : 0.0;
    }

    Optional<SignificanceTester.Result> assess(SignificanceTester tester) {
        if (baselineWindows == null || candidateWindows == null) {
            return Optional.empty();
        }
        return Optional.of(tester.test(baselineWindows, candidateWindows));
    }
}
