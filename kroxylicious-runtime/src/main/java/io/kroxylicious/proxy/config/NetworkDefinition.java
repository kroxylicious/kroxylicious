/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Network configuration for the proxy, allowing Netty settings to be tuned separately
 * for the management endpoint and the proxied Kafka traffic.
 *
 * @param management Netty settings for the management (admin) endpoint
 * @param proxy Netty settings for the proxy's Kafka listeners
 */
public record NetworkDefinition(@Nullable NettySettings management, @Nullable NettySettings proxy) {

}
