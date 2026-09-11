/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.net;

import java.util.Objects;
import java.util.Optional;

/**
 * Represents a network endpoint.  Network endpoints accepts Kafka protocol traffic on behalf of a virtual clusters.
 *
 * @param bindingAddress address of the interface to which the endpoint is bound.  {@link Optional#empty()} indicates the 'any' address.
 * @param port port number
 * @param tls true if TLS is in use for this endpoint.
 */
public record Endpoint(Optional<String> bindingAddress, int port, boolean tls) {
    /**
     * Creates an endpoint.
     *
     * @param bindingAddress address of the interface to which the endpoint is bound.  {@link Optional#empty()} indicates the 'any' address.
     * @param port port number
     * @param tls true if TLS is in use for this endpoint.
     */
    public Endpoint {
        Objects.requireNonNull(bindingAddress);
    }

    /**
     * Creates an endpoint bound to a specific interface.
     *
     * @param bindingAddress address of the interface to which the endpoint is bound.  {@link Optional#empty()} indicates the 'any' address.
     * @param port port number
     * @param tls true if TLS is in use for this endpoint.
     * @return the endpoint
     */
    public static Endpoint createEndpoint(Optional<String> bindingAddress, int port, boolean tls) {
        return new Endpoint(bindingAddress, port, tls);
    }

    /**
     * Creates an endpoint bound to the 'any' address.
     *
     * @param port port number
     * @param tls true if TLS is in use for this endpoint.
     * @return the endpoint
     */
    public static Endpoint createEndpoint(int port, boolean tls) {
        return createEndpoint(Optional.empty(), port, tls);
    }

}
