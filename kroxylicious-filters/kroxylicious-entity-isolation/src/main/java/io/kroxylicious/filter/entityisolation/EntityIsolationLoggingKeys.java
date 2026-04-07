/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.entityisolation;

/**
 * Common keys for structured logging in kroxylicious-entity-isolation.
 */
public class EntityIsolationLoggingKeys {

    private EntityIsolationLoggingKeys() {
    }

    /**
     * The error reported by the filter or underlying system.
     */
    public static final String ERROR = "error";

    /**
     * Failures encountered during topic name mapping.
     */
    public static final String FAILURES = "failures";

    /**
     * The Kafka group ID for consumer groups.
     */
    public static final String GROUP_ID = "groupId";

    /**
     * The Kafka transactional ID.
     */
    public static final String TRANSACTIONAL_ID = "transactionalId";
    /**
     * The api key.
     */
    public static final String API_KEY = "apiKey";

    /**
     * The api version.
     */
    public static final String API_VERSION = "apiVersion";

    /**
     * The max version.
     */
    public static final String MAX_VERSION = "maxVersion";

    /**
     * The message.
     */
    public static final String MESSAGE = "message";

    /**
     * The min version.
     */
    public static final String MIN_VERSION = "minVersion";

    /**
     * The session id.
     */
    public static final String SESSION_ID = "sessionId";

    /**
     * The subject.
     */
    public static final String SUBJECT = "subject";

}
