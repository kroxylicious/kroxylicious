/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import io.kroxylicious.proxy.filter.FilterContext;

/**
 * Describes the possible states of a Kafka session as viewed from the proxy
 * Generally we expect a Kafka Session to transition through one of the following sequences.
 * The simplest model where there are is no detectable authentication: {@code ESTABLISHING -> NOT_AUTHENTICATED -> TERMINATING}
 * The second workflow where we can detect a set of client credentials from M_TLS
 * {@code ESTABLISHING -> NOT_AUTHENTICATED -> TRANSPORT_AUTHENTICATED -> TERMINATING}
 * The third workflow where we can detect a set of client credentials via {@link FilterContext#clientSaslAuthenticationFailure(String, String, Exception)}):
 * {@code ESTABLISHING -> NOT_AUTHENTICATED -> SASL_AUTHENTICATED -> TERMINATING}
 *
 * Certain deployments will configure both M_TLS and SASL Inspection. Sessions in those deployments will go through the expected states.
 * {@code ESTABLISHING -> NOT_AUTHENTICATED -> TRANSPORT_AUTHENTICATED -> SASL_AUTHENTICATED -> TERMINATING}
 *
 * Note the session can transition to {@code TERMINATING} from any other state at any time.
 */
public enum KafkaSessionState {
    /**
     * The proxy has seen a connection, but it is not yet connected to an upstream node and ready for RPCs.
     */
    ESTABLISHING,
    /**
     * The proxy has established TCP connections to both client and broker. It is still prior to any M_TLS or SASL authentication happening.
     * If the proxy can not build an identified subject, are no client TLS certificates or SASL credentials supplied (or no SASL inspector is installed) then the session will remain in this state until it terminating.
     */
    NOT_AUTHENTICATED,
    /**
     * The client has been successfully authenticated based on details of network connection.
     * <p>
     * Typically, this would be from a client TLS certificate.
     */
    TRANSPORT_AUTHENTICATED,
    /**
     * The client has been successfully authenticated using SASLS credentials (at least once).
     */
    SASL_AUTHENTICATED,
    /**
     * The session is being torn down by the proxy.
     */
    TERMINATING
}
