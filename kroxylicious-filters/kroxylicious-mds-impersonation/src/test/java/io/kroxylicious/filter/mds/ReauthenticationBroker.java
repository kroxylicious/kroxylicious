/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.mds;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.ListGroupsResponseData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.message.SaslAuthenticateResponseData;
import org.apache.kafka.common.message.SaslHandshakeResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.ssl.SslContext;

import io.kroxylicious.testing.integration.codec.DecodedRequestFrame;
import io.kroxylicious.testing.integration.codec.DecodedResponseFrame;
import io.kroxylicious.testing.integration.codec.KafkaRequestDecoder;
import io.kroxylicious.testing.integration.codec.KafkaResponseEncoder;

/** Broker wire peer that interleaves an application response with a controlled reauthentication. */
final class ReauthenticationBroker implements AutoCloseable {
    final AtomicInteger connections = new AtomicInteger();
    final List<ApiKeys> requests = new CopyOnWriteArrayList<>();
    final CompletableFuture<Void> applicationPending = new CompletableFuture<>();
    final CompletableFuture<Void> handshakePending = new CompletableFuture<>();
    final CompletableFuture<Void> authenticationPending = new CompletableFuture<>();
    final CompletableFuture<Void> releaseHandshake = new CompletableFuture<>();
    final CompletableFuture<Void> releaseAuthentication = new CompletableFuture<>();
    private final NioEventLoopGroup group = new NioEventLoopGroup(1);
    private final Channel server;

    ReauthenticationBroker(SslContext tls) throws InterruptedException {
        server = new ServerBootstrap().group(group).channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel channel) {
                        connections.incrementAndGet();
                        channel.pipeline().addLast(tls.newHandler(channel.alloc()), new KafkaRequestDecoder(), new KafkaResponseEncoder(), new Handler());
                    }
                }).bind("localhost", 0).sync().channel();
    }

    int port() {
        return ((InetSocketAddress) server.localAddress()).getPort();
    }

    private final class Handler extends SimpleChannelInboundHandler<DecodedRequestFrame<?>> {
        private int handshakes;
        private int authentications;
        private DecodedRequestFrame<?> pendingApplication;

        // Completion callbacks only enqueue writes on the broker event loop; failed writes close the channel.
        @Override
        @SuppressWarnings("FutureReturnValueIgnored")
        protected void channelRead0(ChannelHandlerContext context, DecodedRequestFrame<?> frame) {
            requests.add(frame.apiKey());
            switch (frame.apiKey()) {
                case API_VERSIONS -> respond(context, frame, versions());
                case SASL_HANDSHAKE -> {
                    var reply = new SaslHandshakeResponseData().setMechanisms(List.of("OAUTHBEARER"));
                    if (++handshakes == 1) {
                        respond(context, frame, reply);
                    }
                    else {
                        respond(context, pendingApplication, groups(pendingApplication));
                        releaseHandshake.thenRun(() -> context.executor().execute(() -> respond(context, frame, reply)));
                        handshakePending.complete(null);
                    }
                }
                case SASL_AUTHENTICATE -> {
                    var reply = new SaslAuthenticateResponseData().setSessionLifetimeMs(8000);
                    if (++authentications == 1) {
                        respond(context, frame, reply);
                    }
                    else {
                        releaseAuthentication.thenRun(() -> context.executor().execute(() -> respond(context, frame, reply)));
                        authenticationPending.complete(null);
                    }
                }
                case LIST_GROUPS -> {
                    if (pendingApplication == null) {
                        pendingApplication = frame;
                        applicationPending.complete(null);
                    }
                    else {
                        respond(context, frame, groups(frame));
                    }
                }
                case PRODUCE -> {
                    // The test sends acks=0, so there is no response.
                }
                default -> throw new IllegalStateException("Unexpected API " + frame.apiKey());
            }
        }
    }

    private static ListGroupsResponseData groups(DecodedRequestFrame<?> request) {
        return new ListGroupsResponseData().setGroups(List.of(new ListGroupsResponseData.ListedGroup().setGroupId(request.header().clientId())));
    }

    private static ApiVersionsResponseData versions() {
        var versions = new ApiVersionsResponseData.ApiVersionCollection();
        for (ApiKeys key : ApiKeys.values()) {
            if (key.oldestVersion() >= 0) {
                versions.add(new ApiVersionsResponseData.ApiVersion().setApiKey(key.id).setMinVersion(key.oldestVersion()).setMaxVersion(key.latestVersion()));
            }
        }
        return new ApiVersionsResponseData().setApiKeys(versions);
    }

    // CLOSE_ON_FAILURE handles write failures without blocking the event loop.
    @SuppressWarnings("FutureReturnValueIgnored")
    private static void respond(ChannelHandlerContext context, DecodedRequestFrame<?> request, ApiMessage response) {
        context.writeAndFlush(new DecodedResponseFrame<>(request.apiVersion(), request.correlationId(),
                new ResponseHeaderData().setCorrelationId(request.correlationId()), response)).addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
    }

    @Override
    public void close() {
        server.close().syncUninterruptibly();
        group.shutdownGracefully(0, 5, TimeUnit.SECONDS).syncUninterruptibly();
    }
}
