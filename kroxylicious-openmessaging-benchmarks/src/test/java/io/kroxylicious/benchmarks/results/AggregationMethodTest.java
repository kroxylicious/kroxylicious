/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.benchmarks.results;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.data.Offset.offset;

class AggregationMethodTest {

    @Nested
    class Mean {

        @Test
        void computesMeanOfValues() {
            assertThat(AggregationMethod.MEAN.aggregate(new double[]{ 2.0, 4.0, 6.0 }))
                    .isCloseTo(4.0, offset(0.001));
        }

        @Test
        void returnsZeroForNull() {
            assertThat(AggregationMethod.MEAN.aggregate(null)).isEqualTo(0.0);
        }

        @Test
        void returnsZeroForEmpty() {
            assertThat(AggregationMethod.MEAN.aggregate(new double[0])).isEqualTo(0.0);
        }

        @Test
        void singleElement() {
            assertThat(AggregationMethod.MEAN.aggregate(new double[]{ 7.5 })).isEqualTo(7.5);
        }
    }

    @Nested
    class Median {

        @Test
        void computesMedianOfOddLength() {
            assertThat(AggregationMethod.MEDIAN.aggregate(new double[]{ 3.0, 1.0, 2.0 }))
                    .isEqualTo(2.0);
        }

        @Test
        void computesMedianOfEvenLength() {
            assertThat(AggregationMethod.MEDIAN.aggregate(new double[]{ 4.0, 1.0, 3.0, 2.0 }))
                    .isCloseTo(2.5, offset(0.001));
        }

        @Test
        void returnsZeroForNull() {
            assertThat(AggregationMethod.MEDIAN.aggregate(null)).isEqualTo(0.0);
        }

        @Test
        void returnsZeroForEmpty() {
            assertThat(AggregationMethod.MEDIAN.aggregate(new double[0])).isEqualTo(0.0);
        }

        @Test
        void singleElement() {
            assertThat(AggregationMethod.MEDIAN.aggregate(new double[]{ 5.0 })).isEqualTo(5.0);
        }

        @Test
        void doesNotMutateInput() {
            double[] input = { 3.0, 1.0, 2.0 };
            AggregationMethod.MEDIAN.aggregate(input);
            assertThat(input).containsExactly(3.0, 1.0, 2.0);
        }
    }

    @Nested
    class Max {

        @Test
        void computesMaxOfValues() {
            assertThat(AggregationMethod.MAX.aggregate(new double[]{ 2.0, 6.0, 4.0 }))
                    .isEqualTo(6.0);
        }

        @Test
        void returnsZeroForNull() {
            assertThat(AggregationMethod.MAX.aggregate(null)).isEqualTo(0.0);
        }

        @Test
        void returnsZeroForEmpty() {
            assertThat(AggregationMethod.MAX.aggregate(new double[0])).isEqualTo(0.0);
        }

        @Test
        void singleElement() {
            assertThat(AggregationMethod.MAX.aggregate(new double[]{ 9.0 })).isEqualTo(9.0);
        }
    }
}
