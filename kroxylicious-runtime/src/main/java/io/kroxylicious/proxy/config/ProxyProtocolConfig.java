/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config;

/**
 * Configuration for the HaProxy PROXY protocol.
 * <p>
 * Supports three modes via the {@code mode} property:
 * <ul>
 *   <li>{@link ProxyProtocolMode#REQUIRED REQUIRED} — every connection must begin with a PROXY header</li>
 *   <li>{@link ProxyProtocolMode#AUTO AUTO} — auto-detect whether a PROXY header is present</li>
 *   <li>{@link ProxyProtocolMode#DISABLED DISABLED} — no PROXY protocol handling (default)</li>
 * </ul>
 *
 * @param mode the proxy protocol mode
 * @see <a href="https://www.haproxy.org/download/3.3/doc/proxy-protocol.txt">PROXY protocol specification</a>
 */
public record ProxyProtocolConfig(ProxyProtocolMode mode) {}
