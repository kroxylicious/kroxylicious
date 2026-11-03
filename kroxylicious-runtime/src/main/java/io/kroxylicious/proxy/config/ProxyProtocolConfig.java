/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config;

import java.util.Objects;

/**
 * Per-gateway configuration for the HaProxy PROXY protocol.
 * <p>
 * Supports two modes via the {@code mode} property:
 * <ul>
 *   <li>{@link ProxyProtocolMode#REQUIRED REQUIRED} — every connection must begin with a PROXY header</li>
 *   <li>{@link ProxyProtocolMode#FORBIDDEN FORBIDDEN} — PROXY headers are not permitted (default when block is absent)</li>
 * </ul>
 * <p>
 * A {@code proxyProtocol: {}} block without a {@code mode} key is a configuration error.
 *
 * @param mode the proxy protocol mode; must not be null
 * @see <a href="https://www.haproxy.org/download/3.3/doc/proxy-protocol.txt">PROXY protocol specification</a>
 */
public record ProxyProtocolConfig(ProxyProtocolMode mode) {
    /**
     * Per-gateway configuration for the HaProxy PROXY protocol.
     *
     * @param mode the proxy protocol mode; must not be null
     */
    public ProxyProtocolConfig {
        Objects.requireNonNull(mode, "proxyProtocol.mode must be specified");
    }
}
