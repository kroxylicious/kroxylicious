/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config;

import java.util.EnumSet;
import java.util.Locale;
import java.util.Objects;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Per-gateway configuration for the PROXY protocol.
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
public record ProxyProtocolConfig(@JsonProperty(required = true) ProxyProtocolMode mode) {

    private static final String ALL_NAMES_LOWERED = EnumSet.allOf(ProxyProtocolMode.class).stream()
            .map(m -> m.name().toLowerCase(Locale.ROOT))
            .collect(Collectors.joining(", "));

    /**
     * Per-gateway configuration for the PROXY protocol.
     *
     * @param mode the proxy protocol mode; must not be null
     */
    public ProxyProtocolConfig {
        Objects.requireNonNull(mode, "proxyProtocol.mode must be specified; set it to one of: [%s]".formatted(ALL_NAMES_LOWERED));
    }
}
