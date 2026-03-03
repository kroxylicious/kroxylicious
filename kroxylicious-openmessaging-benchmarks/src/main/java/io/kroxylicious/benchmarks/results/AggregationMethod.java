/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.benchmarks.results;

import java.util.Arrays;

/**
 * Defines how per-interval array metrics from an OMB result are reduced
 * to a single scalar value for comparison purposes.
 */
public enum AggregationMethod {

    MEAN {
        @Override
        public double aggregate(double[] values) {
            if (values == null || values.length == 0) {
                return 0.0;
            }
            return Arrays.stream(values).average().orElse(0.0);
        }
    },

    MEDIAN {
        @Override
        public double aggregate(double[] values) {
            if (values == null || values.length == 0) {
                return 0.0;
            }
            double[] sorted = values.clone();
            Arrays.sort(sorted);
            int mid = sorted.length / 2;
            if (sorted.length % 2 == 0) {
                return (sorted[mid - 1] + sorted[mid]) / 2.0;
            }
            return sorted[mid];
        }
    },

    MAX {
        @Override
        public double aggregate(double[] values) {
            if (values == null || values.length == 0) {
                return 0.0;
            }
            return Arrays.stream(values).max().orElse(0.0);
        }
    };

    /**
     * Reduces an array of per-interval measurements to a single value.
     *
     * @param values the per-interval measurements, may be null or empty
     * @return the aggregated value, or 0.0 if the input is null or empty
     */
    public abstract double aggregate(double[] values);
}
