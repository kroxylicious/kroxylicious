/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.net;

import java.util.concurrent.CompletionStage;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Used by the {@link io.kroxylicious.proxy.internal.KafkaProxyInitializer} to resolve incoming channel
 * metadata into a {@link EndpointBinding}.
 */
public interface EndpointBindingResolver {

    /**
     * Uses channel metadata from the incoming connection to resolve a {@link EndpointBinding}.
     *
     * @param endpoint endpoint being resolved
     * @param sniHostname SNI hostname, may be null.
     * @return completion stage that when complete will yield a {@link EndpointBinding}.
     */
    CompletionStage<EndpointBinding> resolve(Endpoint endpoint, @Nullable String sniHostname);
}
