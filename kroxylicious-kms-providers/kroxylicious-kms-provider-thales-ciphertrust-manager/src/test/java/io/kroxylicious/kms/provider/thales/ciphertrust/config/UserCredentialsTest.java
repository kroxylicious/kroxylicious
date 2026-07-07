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

import io.kroxylicious.proxy.config.secret.InlinePassword;
import io.kroxylicious.proxy.config.secret.PasswordProvider;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class UserCredentialsTest {

    @ParameterizedTest
    @MethodSource("invalidConstructorArguments")
    void constructorValidation(String username, PasswordProvider password, Class<? extends Exception> expectedExceptionType, String expectedMessage) {
        // When/Then
        assertThatThrownBy(() -> new UserCredentials(username, password))
                .isInstanceOf(expectedExceptionType)
                .hasMessageContaining(expectedMessage);
    }

    static Stream<Arguments> invalidConstructorArguments() {
        PasswordProvider validPassword = new InlinePassword("validPassword");
        return Stream.of(
                Arguments.of(null, validPassword, NullPointerException.class, "username cannot be null"),
                Arguments.of("validUser", null, NullPointerException.class, "password cannot be null"),
                Arguments.of("", validPassword, IllegalArgumentException.class, "username cannot be empty"));
    }

    @Test
    void toStringMasksPassword() {
        // Given
        var credentials = new UserCredentials("testuser", new InlinePassword("secret"));

        // When
        String result = credentials.toString();

        // Then
        assertThat(result)
                .contains("testuser")
                .contains("***")
                .doesNotContain("secret");
    }

}
