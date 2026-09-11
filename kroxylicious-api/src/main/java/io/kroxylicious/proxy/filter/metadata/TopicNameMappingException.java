/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.metadata;

import java.util.Objects;

import io.kroxylicious.kafka.common.protocol.Errors;

/**
 * Indicates there was some problem obtaining a name for a topic id
 */
public class TopicNameMappingException extends RuntimeException {
    /**
     * The kafka error underlying this exception.
     */
    private final Errors error;

    /**
     * Creates a new exception for the given error, using the error's default message and exception.
     * @param error the kafka error underlying this exception
     */
    public TopicNameMappingException(Errors error) {
        this(error, error.message());
    }

    /**
     * Creates a new exception for the given error with a custom message and cause.
     * @param error the kafka error underlying this exception
     * @param message the detail message
     */
    public TopicNameMappingException(Errors error, String message) {
        super(message);
        this.error = Objects.requireNonNull(error);
    }

    /**
     * Creates a new exception for the given error with a custom message and cause.
     * @param error the kafka error underlying this exception
     * @param message the detail message
     * @param cause the cause
     */
    public TopicNameMappingException(Errors error, String message, Throwable cause) {
        super(message, cause);
        this.error = Objects.requireNonNull(error);
    }

    /**
     * Returns the kafka error underlying this exception.
     * @return the kafka error underlying this exception
     */
    public Errors getError() {
        return error;
    }
}
