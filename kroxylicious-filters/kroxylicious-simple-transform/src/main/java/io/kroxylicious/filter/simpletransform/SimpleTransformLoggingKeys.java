/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.simpletransform;

/**
 * Common keys for structured logging in kroxylicious-simple-transform.
 */
public class SimpleTransformLoggingKeys {

    private SimpleTransformLoggingKeys() {
    }

    /**
     * Error message or exception description.
     */
    public static final String ERROR = "error";

    /**
     * The Kafka topic name.
     */
    public static final String TOPIC_NAME = "topicName";
}
