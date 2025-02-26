/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config;

import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class NamedRangeTest {

    @Test
    void contains() {
        assertThat(new NamedRange("myrange", 1, 3).values()).containsExactlyInAnyOrder(1, 2, 3);
    }

    @Test
    void singleton() {
        assertThat(new NamedRange("myrange", 1, 1).values()).containsExactlyInAnyOrder(1);
    }

    @Test
    void endBeforeStartIllegal() {
        assertThatThrownBy(() -> new NamedRange("myrange", 1, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("end of range: 0 is before start of range: 1");
    }

    @Test
    void toStringComposition() {
        assertThat(new NamedRange("foo", 1, 2))
                .hasToString("NamedRange{name='foo', start=1, end=2}");
    }

    static Stream<Arguments> isEndBeforeStartOf() {
        return Stream.of(
                arguments(new NamedRange("myrange", 0, 0), new NamedRange("myrange", 1, 1), true),
                arguments(new NamedRange("myrange", 0, 0), new NamedRange("myrange", 2, 2), true),
                arguments(new NamedRange("myrange", 4, 4), new NamedRange("myrange", 2, 2), false),
                arguments(new NamedRange("myrange", 3, 4), new NamedRange("myrange", 2, 3), false),
                arguments(new NamedRange("myrange", 0, 1), new NamedRange("myrange", 1, 1), false),
                arguments(new NamedRange("myrange", 0, 2), new NamedRange("myrange", 1, 1), false),
                arguments(new NamedRange("myrange", 0, 3), new NamedRange("myrange", 1, 1), false));
    }

    @ParameterizedTest
    @MethodSource
    void isEndBeforeStartOf(NamedRange a, NamedRange b, boolean expected) {
        assertThat(a.isEndBeforeStartOf(b)).isEqualTo(expected);
    }

    static Stream<Arguments> isDistinctFrom() {
        return Stream.of(
                arguments(new NamedRange("myrange", 0, 0), new NamedRange("myrange", 1, 1), true),
                arguments(new NamedRange("myrange", 0, 0), new NamedRange("myrange", 2, 2), true),
                arguments(new NamedRange("myrange", 4, 4), new NamedRange("myrange", 2, 2), true),
                arguments(new NamedRange("myrange", 3, 3), new NamedRange("myrange", 2, 3), false),
                arguments(new NamedRange("myrange", 0, 1), new NamedRange("myrange", 1, 1), false),
                arguments(new NamedRange("myrange", 0, 2), new NamedRange("myrange", 1, 1), false),
                arguments(new NamedRange("myrange", 0, 3), new NamedRange("myrange", 1, 1), false));
    }

    @ParameterizedTest
    @MethodSource
    void isDistinctFrom(NamedRange a, NamedRange b, boolean expected) {
        assertThat(a.isDistinctFrom(b)).isEqualTo(expected);
    }
}
