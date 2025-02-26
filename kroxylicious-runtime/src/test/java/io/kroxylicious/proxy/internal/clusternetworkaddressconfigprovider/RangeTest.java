/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider;

import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class RangeTest {

    @Test
    void contains() {
        assertThat(new Range(1, 4).values()).containsExactlyInAnyOrder(1, 2, 3);
    }

    @Test
    void oneItemRange() {
        assertThat(new Range(1, 2).values()).containsExactlyInAnyOrder(1);
    }

    @Test
    void endBeforeStartIllegal() {
        assertThatThrownBy(() -> new Range(1, 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("end of range: 1 (exclusive) is before start of range: 1 (inclusive)");
    }

    static Stream<Arguments> isEndBeforeStartOf() {
        return Stream.of(
                arguments(new Range(0, 1), new Range(1, 2), true),
                arguments(new Range(0, 1), new Range(2, 3), true),
                arguments(new Range(4, 5), new Range(2, 3), false),
                arguments(new Range(3, 5), new Range(2, 4), false),
                arguments(new Range(0, 2), new Range(1, 2), false),
                arguments(new Range(0, 3), new Range(1, 2), false),
                arguments(new Range(0, 4), new Range(1, 2), false));
    }

    @ParameterizedTest
    @MethodSource
    void isEndBeforeStartOf(Range a, Range b, boolean expected) {
        assertThat(a.isEndBeforeStartOf(b)).isEqualTo(expected);
    }

    static Stream<Arguments> isDistinctFrom() {
        return Stream.of(
                arguments(new Range(0, 1), new Range(1, 2), true),
                arguments(new Range(0, 1), new Range(2, 3), true),
                arguments(new Range(4, 5), new Range(2, 3), true),
                arguments(new Range(3, 5), new Range(2, 4), false),
                arguments(new Range(0, 2), new Range(1, 2), false),
                arguments(new Range(0, 3), new Range(1, 2), false),
                arguments(new Range(0, 4), new Range(1, 2), false));
    }

    @ParameterizedTest
    @MethodSource
    void isDistinctFrom(Range a, Range b, boolean expected) {
        assertThat(a.isDistinctFrom(b)).isEqualTo(expected);
    }

}