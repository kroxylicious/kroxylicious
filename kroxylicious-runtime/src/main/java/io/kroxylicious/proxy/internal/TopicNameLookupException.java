/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

/**
 * Indicates there was some problem obtaining a name for a topic id
 */
public class TopicNameLookupException extends RuntimeException {
    public TopicNameLookupException(String message) {
        super(message);
    }

    public TopicNameLookupException(String message, Throwable cause) {
        super(message, cause);
    }
}
