/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.connectionexpiration;

/**
 * Common keys for structured logging in kroxylicious-connection-expiration.
 */
class ConnectionExpirationLoggingKeys {

    private ConnectionExpirationLoggingKeys() {
    }

    /**
     * The Kafka API key identifying the request or response type.
     */
    public static final String API_KEY = "apiKey";

    /**
     * Connection expiration deadline.
     */
    public static final String DEADLINE = "deadline";

    /**
     * The unique identifier for a client session with the proxy.
     */
    public static final String SESSION_ID = "sessionId";
}
