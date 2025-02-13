/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

/**
 * Indicates the proxy could not map a topic id to a topic name
 */
public class TopicIdMappingException extends RuntimeException {
    public TopicIdMappingException(String message) {
        super(message);
    }
}
