/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.net;

import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;

/**
 * Request for a network endpoint to be unbound.
 */
public class NetworkUnbindRequest extends NetworkBindingOperation<Void> {
    private static final Logger LOGGER = LoggerFactory.getLogger(NetworkUnbindRequest.class);
    private final Channel channel;
    private final CompletableFuture<Void> future;

    public NetworkUnbindRequest(boolean tls, Channel channel, CompletableFuture<Void> future) {
        super(tls);
        this.channel = channel;
        this.future = future;
    }

    @Override
    public int port() {
        return ((InetSocketAddress) channel.localAddress()).getPort();
    }

    @Override
    public void performBindingOperation(ServerBootstrap serverBootstrap, ExecutorService executorService) {
        try {
            var addr = channel.localAddress();
            LOGGER.info("Unbinding {}", addr);

            channel.close().addListener((ChannelFutureListener) channelFuture -> {
                executorService.execute(() -> {
                    if (channelFuture.cause() != null) {
                        LOGGER.debug("Unbind failed {}", addr, channelFuture.cause());
                        future.completeExceptionally(channelFuture.cause());
                    } else {
                        LOGGER.info("Unbound {}", addr);
                        future.complete(null);
                    }
                });
            });
        }
        catch (Throwable t) {
            future.completeExceptionally(t);
        }
    }

    @Override
    public CompletableFuture<Void> getFuture() {
        return future;
    }
}
