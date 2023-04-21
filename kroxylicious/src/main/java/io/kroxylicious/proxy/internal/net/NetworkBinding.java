/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.net;

import java.util.Optional;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;

public interface NetworkBinding {
    ChannelFuture bind(ServerBootstrap bootstrap);

    boolean tls();

    static NetworkBinding createNetworkBinding(Optional<String> host, int port, boolean tls) {
        if (host.isPresent()) {
            return new InterfaceSpecificNetworkBinding(host.get(), port, tls);
        }
        else {
            return new AllInterfaceNetworkBinding(port, tls);

        }
    }

    record InterfaceSpecificNetworkBinding(String host, int port, boolean tls) implements NetworkBinding {

    @Override
        public ChannelFuture bind(ServerBootstrap bootstrap) {
            return bootstrap.bind(host, port);
        }}

    record AllInterfaceNetworkBinding(int port, boolean tls) implements NetworkBinding {

    @Override
        public ChannelFuture bind(ServerBootstrap bootstrap) {
            return bootstrap.bind(port);
        }
}

}
