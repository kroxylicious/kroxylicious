/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

/**
 * Describes the possible states of a Kafka session as viewed from the proxy
 */
public enum KafkaSessionState {
    /**
     * Prior to any M_TLS or SASL authentication happening.
     */
    NOT_AUTHENTICATED,
    /**
     * The client has been successfully authenticated (at least once).
     */
    AUTHENTICATED,
    /**
     * The
     */
    AUTHORIZED
}
