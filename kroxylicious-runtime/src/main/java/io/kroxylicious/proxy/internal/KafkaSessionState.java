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
     * The proxy has seen a connection, but it is not yet connected to an upstream node and ready for RPCs.
     */
    ESTABLISHING,
    /**
     * The proxy has established TCP connections to both client and broker. It is still prior to any M_TLS or SASL authentication happening.
     * If there are no client TLS certificates or SASL credentials supplied (or no SASL inspector is installed) then the session will remain in this state until it terminating.
     */
    NOT_AUTHENTICATED,
    /**
     * The client has been successfully authenticated (at least once).
     */
    AUTHENTICATED,
    /**
     * The session is being torn down by the proxy.
     */
    TERMINATING
}
