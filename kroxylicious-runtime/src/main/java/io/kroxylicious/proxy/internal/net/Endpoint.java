/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.net;

import java.util.Objects;
import java.util.Optional;

import io.netty.channel.Channel;
import io.netty.channel.socket.ServerSocketChannel;

/**
 * Represents a network endpoint.  Network endpoints accepts Kafka protocol traffic on behalf of a virtual clusters.
 *
 * @param bindingAddress address of the interface to which the endpoint is bound.  {@link Optional#empty()} indicates the 'any' address.
 * @param port port number
 * @param tls true if TLS is in use for this endpoint.
 */
public record Endpoint(Optional<String> bindingAddress, int port, boolean tls) {
    public Endpoint {
        Objects.requireNonNull(bindingAddress);
    }

    public static Endpoint createEndpoint(Channel ch, boolean tls) {
        try {
            if (ch.parent() instanceof ServerSocketChannel serverSocketChannel) {
                var serverSocketAddress = serverSocketChannel.localAddress();
                var bindingAddress = serverSocketAddress.getAddress().isAnyLocalAddress() ? Optional.<String> empty()
                        : Optional.of(serverSocketAddress.getAddress().getHostAddress());
                return new Endpoint(bindingAddress, serverSocketAddress.getPort(), tls);
            }
            else {
                throw new UnsupportedOperationException(
                        "Channel is either not ServerSocketChannel or the channel/channel parent is null");
            }
        }
        catch (Exception e) {
            throw new EndpointResolutionException("Failed to create endpoint for the channel: " + e.getMessage(), e);
        }
    }

    public static Endpoint createEndpoint(Optional<String> bindingAddress, int port, boolean tls) {
        return new Endpoint(bindingAddress, port, tls);
    }

    public static Endpoint createEndpoint(int port, boolean tls) {
        return createEndpoint(Optional.empty(), port, tls);
    }

}
