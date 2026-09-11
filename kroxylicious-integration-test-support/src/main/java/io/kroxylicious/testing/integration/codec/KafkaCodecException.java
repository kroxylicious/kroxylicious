/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.integration.codec;

/**
 * Thrown when there is a problem translating between binary and Kafka ApiMessages
 */
public class KafkaCodecException extends RuntimeException {
    /**
     * Creates a KafkaCodecException.
     *
     * @param message the detail message
     */
    public KafkaCodecException(String message) {
        super(message);
    }

    /**
     * Creates a KafkaCodecException.
     *
     * @param message the detail message
     * @param cause the underlying cause
     */
    public KafkaCodecException(String message, Throwable cause) {
        super(message, cause);
    }
}
