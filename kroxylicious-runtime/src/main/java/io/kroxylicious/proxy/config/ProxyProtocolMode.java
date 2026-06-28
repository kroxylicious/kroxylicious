/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Controls how Kroxylicious handles HaProxy PROXY protocol headers on incoming connections.
 *
 * @see <a href="https://www.haproxy.org/download/3.3/doc/proxy-protocol.txt">PROXY protocol specification</a>
 */
public enum ProxyProtocolMode {

    /**
     * Every incoming connection <b>must</b> begin with a valid PROXY protocol header (v1 or v2).
     * Connections that do not send a PROXY header will be rejected with a clear error.
     */
    @JsonProperty("required")
    REQUIRED,

    /**
     * Kroxylicious inspects the first bytes of each connection and automatically
     * detects whether a PROXY protocol header is present. If the bytes match a
     * valid v1 or v2 signature the header is decoded and the original client
     * address is extracted; otherwise the bytes are passed through to the Kafka
     * protocol decoder as-is.
     * <p>
     * This mode is useful when the same listener may receive both proxied and
     * direct connections.
     * </p>
     */
    @JsonProperty("allowed")
    ALLOWED,

    /**
     * No PROXY protocol handling. All bytes are treated as Kafka protocol data.
     * This is the default when no {@code proxyProtocol} block is configured.
     */
    @JsonProperty("disabled")
    DISABLED
}
