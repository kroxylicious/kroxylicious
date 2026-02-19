/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config;

/**
 * Configuration for the HAProxy PROXY protocol.
 * <p>
 * When enabled, Kroxylicious expects every incoming connection to begin with a
 * PROXY protocol header (v1 or v2) as sent by an upstream load-balancer.
 * The original client address information is extracted and made available
 * to the proxy session.
 * </p>
 *
 * @param enabled whether PROXY protocol decoding is active
 */
public record ProxyProtocolConfig(boolean enabled) {}
