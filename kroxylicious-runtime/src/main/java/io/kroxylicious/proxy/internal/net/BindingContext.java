/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.net;

import io.kroxylicious.proxy.config.ProxyProtocolMode;

/**
 * Per-acceptor-channel state set by {@link EndpointRegistry} when the first gateway binds
 * to a listening port, and read by {@code KafkaProxyInitializer} at {@code initChannel()}
 * time — before the per-connection binding is resolved.
 *
 * @param proxyProtocolMode the PROXY protocol enforcement mode for this listening port
 */
public record BindingContext(ProxyProtocolMode proxyProtocolMode) {}
