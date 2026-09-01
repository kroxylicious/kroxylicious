/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.metadata;

import org.junit.jupiter.api.Test;

import io.kroxylicious.kafka.common.protocol.Errors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TopicNameMappingExceptionTest {

    @Test
    void errorConstructorUsesDefaultMessageAndNoCause() {
        // Given
        var error = Errors.UNKNOWN_TOPIC_OR_PARTITION;

        // When
        var exception = new TopicNameMappingException(error);

        // Then
        assertThat(exception.getError()).isEqualTo(error);
        assertThat(exception.getMessage()).isEqualTo(error.message());
        assertThat(exception.getCause()).isNull();
    }

    @Test
    void errorAndMessageConstructorUsesCustomMessageAndNoCause() {
        // Given
        var error = Errors.UNKNOWN_TOPIC_OR_PARTITION;

        // When
        var exception = new TopicNameMappingException(error, "custom message");

        // Then
        assertThat(exception.getError()).isEqualTo(error);
        assertThat(exception.getMessage()).isEqualTo("custom message");
        assertThat(exception.getCause()).isNull();
    }

    @Test
    void errorMessageAndCauseConstructorRetainsCause() {
        // Given
        var error = Errors.UNKNOWN_TOPIC_OR_PARTITION;
        var cause = new RuntimeException("boom");

        // When
        var exception = new TopicNameMappingException(error, "custom message", cause);

        // Then
        assertThat(exception.getError()).isEqualTo(error);
        assertThat(exception.getMessage()).isEqualTo("custom message");
        assertThat(exception.getCause()).isSameAs(cause);
    }

    @Test
    void rejectsNullError() {
        // When / Then
        assertThatThrownBy(() -> new TopicNameMappingException(null, "custom message"))
                .isInstanceOf(NullPointerException.class);
    }
}
