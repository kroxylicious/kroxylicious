/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kafka.transform;

/**
 * Common keys for structured logging in kroxylicious-kafka-message-tools.
 */
public class TransformLoggingKeys {

    private TransformLoggingKeys() {
    }

    /**
     * The Kafka API key identifying the request or response type.
     */
    public static final String API_KEY = "apiKey";

    /**
     * Source API version being transformed from.
     */
    public static final String FROM_VERSION = "fromVersion";

    /**
     * Target API version being transformed to.
     */
    public static final String TO_VERSION = "toVersion";
}
