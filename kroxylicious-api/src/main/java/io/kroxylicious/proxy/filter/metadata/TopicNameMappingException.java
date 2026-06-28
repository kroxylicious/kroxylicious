/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.metadata;

import java.util.Objects;

import org.apache.kafka.common.protocol.Errors;

/**
 * Indicates there was some problem obtaining a name for a topic id
 */
public class TopicNameMappingException extends RuntimeException {
    private final Errors error;

    public TopicNameMappingException(Errors error) {
        this(error, error.message(), error.exception());
    }

    public TopicNameMappingException(Errors error, String message) {
        this(error, message, error.exception());
    }

    public TopicNameMappingException(Errors error, String message, Throwable cause) {
        super(message, cause);
        this.error = Objects.requireNonNull(error);
    }

    public Errors getError() {
        return error;
    }
}
