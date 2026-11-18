/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.api.client;

import java.io.Serial;

/**
 * Thrown when a {@link KafkaClient} operation fails, e.g. because the proxy rejected
 * the request or the client could not connect.
 */
public class KafkaClientException extends RuntimeException {

    @Serial
    private static final long serialVersionUID = 1L;

    public KafkaClientException(String message) {
        super(message);
    }

    public KafkaClientException(String message, Throwable cause) {
        super(message, cause);
    }
}
