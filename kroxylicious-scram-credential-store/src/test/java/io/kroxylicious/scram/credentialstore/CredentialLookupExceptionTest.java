/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.scram.credentialstore;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class CredentialLookupExceptionTest {

    private static final String MESSAGE = "lookup failed";

    @Test
    void shouldConstructWithMessage() {
        // When
        var ex = new CredentialLookupException(MESSAGE);

        // Then
        assertThat(ex).hasMessage(MESSAGE).hasNoCause();
    }

    @Test
    void shouldConstructWithMessageAndCause() {
        // Given
        var cause = new RuntimeException("root cause");

        // When
        var ex = new CredentialLookupException(MESSAGE, cause);

        // Then
        assertThat(ex).hasMessage(MESSAGE).hasCause(cause);
    }

    @Test
    void shouldConstructWithCause() {
        // Given
        var cause = new RuntimeException("root cause");

        // When
        var ex = new CredentialLookupException(cause);

        // Then
        assertThat(ex).hasCause(cause);
    }

    @Test
    void unavailableShouldConstructWithMessage() {
        // When
        var ex = new CredentialServiceUnavailableException(MESSAGE);

        // Then
        assertThat(ex).hasMessage(MESSAGE).hasNoCause()
                .isInstanceOf(CredentialLookupException.class);
    }

    @Test
    void unavailableShouldConstructWithMessageAndCause() {
        // Given
        var cause = new RuntimeException("root cause");

        // When
        var ex = new CredentialServiceUnavailableException(MESSAGE, cause);

        // Then
        assertThat(ex).hasMessage(MESSAGE).hasCause(cause);
    }

    @Test
    void unavailableShouldConstructWithCause() {
        // Given
        var cause = new RuntimeException("root cause");

        // When
        var ex = new CredentialServiceUnavailableException(cause);

        // Then
        assertThat(ex).hasCause(cause);
    }

    @Test
    void timeoutShouldConstructWithMessage() {
        // When
        var ex = new CredentialServiceTimeoutException(MESSAGE);

        // Then
        assertThat(ex).hasMessage(MESSAGE).hasNoCause()
                .isInstanceOf(CredentialLookupException.class);
    }

    @Test
    void timeoutShouldConstructWithMessageAndCause() {
        // Given
        var cause = new RuntimeException("root cause");

        // When
        var ex = new CredentialServiceTimeoutException(MESSAGE, cause);

        // Then
        assertThat(ex).hasMessage(MESSAGE).hasCause(cause);
    }

    @Test
    void timeoutShouldConstructWithCause() {
        // Given
        var cause = new RuntimeException("root cause");

        // When
        var ex = new CredentialServiceTimeoutException(cause);

        // Then
        assertThat(ex).hasCause(cause);
    }
}
