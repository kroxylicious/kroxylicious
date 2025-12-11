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
     * The proxy has seen a connection, but it is not yet connected ready for RPCs.
     */
    ESTABLISHING,
    /**
     * The proxy has established connections to both client and broker. It is still prior to any M_TLS or SASL authentication happening.
     */
    NOT_AUTHENTICATED,
    /**
     * The client has been successfully authenticated (at least once).
     */
    AUTHENTICATED,
    /**
     * The client has been Authorized to perform actions.
     */
    AUTHORIZED,
    /**
     * The session is being torn down by the proxy.
     */
    TERMINATING
}
