/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.benchmarks.results;

import java.util.function.BiFunction;

import org.apache.commons.math3.stat.inference.MannWhitneyUTest;

/**
 * Tests whether two samples of per-window latency measurements differ significantly,
 * using the Mann-Whitney U test.
 * <p>
 * The p-value computation is injectable so threshold logic can be tested without
 * depending on Commons Math producing specific values for known inputs.
 */
public class SignificanceTester {

    /** p-value threshold below which a difference is considered statistically significant. */
    static final double SIGNIFICANCE_THRESHOLD = 0.05;

    public record Result(double pValue, boolean significant) {}

    private final BiFunction<double[], double[], Double> pValueComputer;

    /** Production constructor — uses Commons Math Mann-Whitney U test. */
    public SignificanceTester() {
        this(SignificanceTester::mannWhitneyPValue);
    }

    /** Package-private test constructor — allows injecting a stub p-value computer. */
    SignificanceTester(BiFunction<double[], double[], Double> pValueComputer) {
        this.pValueComputer = pValueComputer;
    }

    /**
     * Tests whether the two per-window sample arrays differ significantly.
     *
     * @param baseline  per-window measurements for the baseline
     * @param candidate per-window measurements for the candidate
     * @return result containing the p-value and whether the difference is significant
     */
    public Result test(double[] baseline, double[] candidate) {
        double pValue = pValueComputer.apply(baseline, candidate);
        return new Result(pValue, pValue < SIGNIFICANCE_THRESHOLD);
    }

    private static double mannWhitneyPValue(double[] a, double[] b) {
        return new MannWhitneyUTest().mannWhitneyUTest(a, b);
    }
}
