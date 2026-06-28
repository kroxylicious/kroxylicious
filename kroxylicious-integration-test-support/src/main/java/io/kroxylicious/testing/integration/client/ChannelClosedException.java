/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.integration.client;

/**
 * Exception thrown when a channel closes before queued requests can be sent.
 */
public class ChannelClosedException extends RuntimeException {

    public ChannelClosedException(String message) {
        super(message);
    }
}
