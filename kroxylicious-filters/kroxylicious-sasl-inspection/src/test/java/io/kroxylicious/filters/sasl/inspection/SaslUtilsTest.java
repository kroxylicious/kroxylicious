/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filters.sasl.inspection;

import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SaslUtilsTest {

    static Stream<Arguments> validNames() {
        return Stream.of(Arguments.argumentSet("simple", "bob", "bob"),
                Arguments.argumentSet("with comma", "bob=2Ccomma", "bob,comma"),
                Arguments.argumentSet("with equals", "bob=3Dequals", "bob=equals"),
                Arguments.argumentSet("with many", "bob=3D=3D", "bob=="),
                Arguments.argumentSet("only encoded char", "=3D", "="),
                Arguments.argumentSet("with both", "bob=2C=3D", "bob,="));
    }

    @ParameterizedTest
    @MethodSource
    void validNames(String input, String expectedOutput) {
        assertThat(SaslUtils.decodeSaslName(input))
                .isEqualTo(expectedOutput);
    }

    static Stream<Arguments> invalidNames() {
        return Stream.of(Arguments.argumentSet("unexpected encoded char", "bob=2D"),
                Arguments.argumentSet("capitals ony", "bob=2c"),
                Arguments.argumentSet("many", "=2C=2D"));
    }

    @ParameterizedTest
    @MethodSource
    void invalidNames(String input) {
        assertThatThrownBy(() -> SaslUtils.decodeSaslName(input))
                .isInstanceOf(IllegalArgumentException.class);
    }

}