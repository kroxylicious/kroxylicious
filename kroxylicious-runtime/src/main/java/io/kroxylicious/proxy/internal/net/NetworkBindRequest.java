/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.net;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;

/**
 * Request for a network endpoint to be bound.
 *
 */
public class NetworkBindRequest extends NetworkBindingOperation<Channel> {
    private static final Logger LOGGER = LoggerFactory.getLogger(NetworkBindRequest.class);
    private final CompletableFuture<Channel> future;
    private final Endpoint endpoint;

    public NetworkBindRequest(CompletableFuture<Channel> future, Endpoint endpoint) {
        super(endpoint.tls());
        this.future = future;
        this.endpoint = endpoint;
    }

    public Optional<String> getBindingAddress() {
        return endpoint.bindingAddress();
    }

    @Override
    public int port() {
        return endpoint.port();
    }

    @Override
    public CompletableFuture<Channel> getFuture() {
        return future;
    }

    @Override
    public void performBindingOperation(ServerBootstrap serverBootstrap, ExecutorService executorService) {
        try {
            int port = port();
            var bindingAddress = endpoint.bindingAddress();
            ChannelFuture bind;
            if (bindingAddress.isPresent()) {
                LOGGER.debug("Binding {}:{}", bindingAddress.get(), port);
                bind = serverBootstrap.bind(bindingAddress.get(), port);
            } else {
                LOGGER.debug("Binding <any>:{}", port);
                bind = serverBootstrap.bind(port);
            }
            bind.addListener((ChannelFutureListener) channelFuture -> {
                executorService.execute(() -> {
                    if (channelFuture.cause() != null) {
                        future.completeExceptionally(channelFuture.cause());
                    } else {
                        future.complete(channelFuture.channel());
                    }
                });
            });
        }
        catch (Throwable t) {
            future.completeExceptionally(t);
        }
    }

}
