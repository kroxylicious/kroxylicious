/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Controls how Kroxylicious handles HaProxy PROXY protocol headers on incoming connections.
 * <p>
 * This mode is configured per gateway under
 * {@code virtualClusters[*].gateways[*].proxyProtocol.mode}.
 * When no {@code proxyProtocol} block is present on a gateway, the effective mode is
 * {@link #FORBIDDEN}.
 * </p>
 *
 * @see <a href="https://www.haproxy.org/download/3.3/doc/proxy-protocol.txt">PROXY protocol specification</a>
 */
public enum ProxyProtocolMode {

    /**
     * Every incoming connection <b>must</b> begin with a valid PROXY protocol header (v1 or v2).
     * Connections that do not send a PROXY header will be rejected.
     */
    @JsonProperty("required")
    REQUIRED,

    /**
     * PROXY protocol headers are not permitted. This is the default when no
     * {@code proxyProtocol} block is configured on a gateway.
     * <p>
     * No PROXY-protocol decoder is installed in the pipeline. If a client sends a PROXY
     * header, the raw bytes reach the Kafka frame decoder, which raises a protocol error
     * and closes the connection.
     * </p>
     */
    @JsonProperty("forbidden")
    FORBIDDEN
}
