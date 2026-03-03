/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.test.codec;

/**
 * Thrown when there is a problem translating between binary and Kafka ApiMessages
 */
public class KafkaCodecException extends RuntimeException {
    public KafkaCodecException(String message) {
        super(message);
    }

    public KafkaCodecException(String message, Throwable cause) {
        super(message, cause);
    }
}
