/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter;

/**
 * Indicates there was some problem obtaining topic name mappings
 */
public class TopicNameMappingException extends RuntimeException {
    public TopicNameMappingException(String message) {
        super(message);
    }

    public TopicNameMappingException(String message, Throwable cause) {
        super(message, cause);
    }
}
