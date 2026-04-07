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
    /**
     * The error code.
     */
    public static final String ERROR_CODE = "errorCode";

    /**
     * The failure count.
     */
    public static final String FAILURE_COUNT = "failureCount";

    /**
     * The outcome.
     */
    public static final String OUTCOME = "outcome";

    /**
     * The topic id.
     */
    public static final String TOPIC_ID = "topicId";

    /**
     * The topic ids.
     */
    public static final String TOPIC_IDS = "topicIds";

}
