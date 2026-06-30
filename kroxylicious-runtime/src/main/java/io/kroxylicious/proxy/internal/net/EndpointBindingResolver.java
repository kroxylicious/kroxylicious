/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.net;

import java.util.concurrent.CompletionStage;

import io.netty.channel.Channel;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Used by the {@link io.kroxylicious.proxy.internal.KafkaProxyInitializer} to resolve incoming channel
 * metadata into a {@link EndpointBinding}.
 */
public interface EndpointBindingResolver {

    /**
     * Uses channel metadata from the incoming connection to resolve a {@link EndpointBinding}.
     * The channel's parent (the acceptor) carries the binding information set at bind time.
     *
     * @param channel the child channel representing the accepted connection
     * @param sniHostname SNI hostname, may be null.
     * @return completion stage that when complete will yield a {@link EndpointBinding}.
     */
    CompletionStage<EndpointBinding> resolve(Channel channel, @Nullable String sniHostname);
}
