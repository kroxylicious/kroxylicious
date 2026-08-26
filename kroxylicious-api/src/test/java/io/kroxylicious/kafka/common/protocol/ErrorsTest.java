/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kafka.common.protocol;

import java.util.HashSet;
import java.util.Set;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.assertj.core.api.Assertions.assertThat;

class ErrorsTest {

    @Test
    void shouldHaveUniqueErrorCodes() {
        Set<Short> codeSet = new HashSet<>();
        for (Errors error : Errors.values()) {
            codeSet.add(error.code());
        }
        assertThat(codeSet).hasSize(Errors.values().length);
    }

    @ParameterizedTest
    @EnumSource(Errors.class)
    void shouldHaveAMessage(Errors error) {
        assertThat(error.message()).isNotNull().isNotBlank();
    }

    @ParameterizedTest
    @EnumSource(Errors.class)
    void shouldMapCodeToEnum(Errors error) {
        assertThat(Errors.forCode(error.code())).isEqualTo(error);
    }
}