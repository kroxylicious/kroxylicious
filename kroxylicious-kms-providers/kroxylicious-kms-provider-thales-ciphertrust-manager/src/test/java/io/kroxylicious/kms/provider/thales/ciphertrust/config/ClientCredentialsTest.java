/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.thales.ciphertrust.config;

import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ClientCredentialsTest {

    @ParameterizedTest
    @MethodSource("invalidConstructorArguments")
    void constructorValidation(String clientId, Class<? extends Exception> expectedExceptionType, String expectedMessage) {
        // When/Then
        assertThatThrownBy(() -> new ClientCredentials(clientId))
                .isInstanceOf(expectedExceptionType)
                .hasMessageContaining(expectedMessage);
    }

    static Stream<Arguments> invalidConstructorArguments() {
        return Stream.of(
                Arguments.of(null, NullPointerException.class, "clientId cannot be null"),
                Arguments.of("", IllegalArgumentException.class, "clientId cannot be blank"),
                Arguments.of("   ", IllegalArgumentException.class, "clientId cannot be blank"));
    }

    @Test
    void toStringMasksClientId() {
        // Given
        var credentials = new ClientCredentials("secret-client-id-123");

        // When
        String result = credentials.toString();

        // Then
        assertThat(result)
                .contains("***")
                .doesNotContain("secret-client-id-123");
    }
}
