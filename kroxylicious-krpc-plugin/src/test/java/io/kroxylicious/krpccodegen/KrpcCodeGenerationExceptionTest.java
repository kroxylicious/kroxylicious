/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.krpccodegen;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class KrpcCodeGenerationExceptionTest {

    @Test
    void shouldCreateExceptionWithMessage() {
        var exception = new KrpcCodeGenerationException("test message");
        assertThat(exception.getMessage()).isEqualTo("test message");
    }

    @Test
    void shouldCreateExceptionWithCause() {
        var cause = new RuntimeException("cause");
        var exception = new KrpcCodeGenerationException(cause);
        assertThat(exception.getCause()).isEqualTo(cause);
    }

    @Test
    void shouldCreateExceptionWithMessageAndCause() {
        var cause = new RuntimeException("cause");
        var exception = new KrpcCodeGenerationException("test message", cause);
        assertThat(exception.getMessage()).isEqualTo("test message");
        assertThat(exception.getCause()).isEqualTo(cause);
    }
}