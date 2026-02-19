/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.it;

import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.github.nettyplus.leakdetector.junit.NettyLeakDetectorExtension;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.haproxy.HAProxyCommand;
import io.netty.handler.codec.haproxy.HAProxyMessage;
import io.netty.handler.codec.haproxy.HAProxyMessageEncoder;
import io.netty.handler.codec.haproxy.HAProxyProtocolVersion;
import io.netty.handler.codec.haproxy.HAProxyProxiedProtocol;

import io.kroxylicious.proxy.config.ProxyProtocolConfig;
import io.kroxylicious.proxy.config.ProxyProtocolMode;
import io.kroxylicious.test.Request;
import io.kroxylicious.test.Response;
import io.kroxylicious.test.ResponsePayload;
import io.kroxylicious.test.tester.KroxyliciousConfigUtils;

import static io.kroxylicious.test.tester.KroxyliciousTesters.mockKafkaKroxyliciousTester;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for HAProxy PROXY protocol support.
 */
@ExtendWith(NettyLeakDetectorExtension.class)
class ProxyProtocolIT {

    // ---- REQUIRED mode tests ----

    @Test
    void requiredModeShouldRejectConnectionWithoutProxyHeader() {
        // Given - proxy protocol is required but client sends a normal Kafka request
        try (var tester = mockKafkaKroxyliciousTester(bootstrap -> KroxyliciousConfigUtils.proxy(bootstrap)
                .withProxyProtocol(new ProxyProtocolConfig(ProxyProtocolMode.REQUIRED)));
                var client = tester.simpleTestClient()) {

            var request = new Request(ApiKeys.API_VERSIONS, ApiVersionsRequestData.HIGHEST_SUPPORTED_VERSION,
                    "test-client", new ApiVersionsRequestData());

            // When / Then - the request should fail fast (not hang)
            assertThat(client.get(request))
                    .failsWithin(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void requiredModeShouldAcceptConnectionWithProxyHeader() throws Exception {
        // Given - proxy protocol is required
        try (var tester = mockKafkaKroxyliciousTester(bootstrap -> KroxyliciousConfigUtils.proxy(bootstrap)
                .withProxyProtocol(new ProxyProtocolConfig(ProxyProtocolMode.REQUIRED)))) {

            var address = tester.getBootstrapAddress();
            var parts = address.split(":");
            var host = parts[0];
            var port = Integer.parseInt(parts[1]);

            var group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
            try {
                Channel channel = connectAndSendProxyHeader(group, host, port);

                // Then - the channel should remain active (proxy accepted the PROXY header)
                Thread.sleep(200);
                assertThat(channel.isActive())
                        .as("Channel should remain active after PROXY header is accepted")
                        .isTrue();

                channel.close().sync();
            }
            finally {
                group.shutdownGracefully(0, 1, TimeUnit.SECONDS).sync();
            }
        }
    }

    // ---- AUTO mode tests ----

    @Test
    void autoModeShouldAcceptConnectionWithProxyHeader() throws Exception {
        // Given - proxy protocol is auto
        try (var tester = mockKafkaKroxyliciousTester(bootstrap -> KroxyliciousConfigUtils.proxy(bootstrap)
                .withProxyProtocol(new ProxyProtocolConfig(ProxyProtocolMode.AUTO)))) {

            var address = tester.getBootstrapAddress();
            var parts = address.split(":");
            var host = parts[0];
            var port = Integer.parseInt(parts[1]);

            var group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
            try {
                Channel channel = connectAndSendProxyHeader(group, host, port);

                Thread.sleep(200);
                assertThat(channel.isActive())
                        .as("Channel should remain active after PROXY header is accepted in auto mode")
                        .isTrue();

                channel.close().sync();
            }
            finally {
                group.shutdownGracefully(0, 1, TimeUnit.SECONDS).sync();
            }
        }
    }

    @Test
    void autoModeShouldAcceptDirectConnectionWithoutProxyHeader() {
        // Given - proxy protocol is auto, client connects directly without PROXY header
        try (var tester = mockKafkaKroxyliciousTester(bootstrap -> KroxyliciousConfigUtils.proxy(bootstrap)
                .withProxyProtocol(new ProxyProtocolConfig(ProxyProtocolMode.AUTO)));
                var client = tester.simpleTestClient()) {

            tester.addMockResponseForApiKey(new ResponsePayload(ApiKeys.API_VERSIONS,
                    ApiVersionsRequestData.HIGHEST_SUPPORTED_VERSION, new ApiVersionsResponseData()));

            var request = new Request(ApiKeys.API_VERSIONS, ApiVersionsRequestData.HIGHEST_SUPPORTED_VERSION,
                    "test-client", new ApiVersionsRequestData());

            // When / Then - normal request should succeed (auto mode passes through)
            Response response = client.getSync(request);
            assertThat(response.payload().message()).isInstanceOf(ApiVersionsResponseData.class);
        }
    }

    // ---- DISABLED mode tests ----

    @Test
    void disabledModeShouldWorkNormally() {
        // Given - proxy protocol is NOT enabled (default)
        try (var tester = mockKafkaKroxyliciousTester(KroxyliciousConfigUtils::proxy);
                var client = tester.simpleTestClient()) {

            tester.addMockResponseForApiKey(new ResponsePayload(ApiKeys.API_VERSIONS,
                    ApiVersionsRequestData.HIGHEST_SUPPORTED_VERSION, new ApiVersionsResponseData()));

            var request = new Request(ApiKeys.API_VERSIONS, ApiVersionsRequestData.HIGHEST_SUPPORTED_VERSION,
                    "test-client", new ApiVersionsRequestData());

            // When / Then - normal request should succeed
            Response response = client.getSync(request);
            assertThat(response.payload().message()).isInstanceOf(ApiVersionsResponseData.class);
        }
    }

    // ---- Connection-not-stuck tests ----

    @Test
    void requiredModeShouldNotHangWhenNonProxyBytesReceived() {
        // Given - proxy protocol required, client sends raw bytes (not PROXY header)
        // This test ensures the connection is rejected quickly rather than hanging indefinitely.
        try (var tester = mockKafkaKroxyliciousTester(bootstrap -> KroxyliciousConfigUtils.proxy(bootstrap)
                .withProxyProtocol(new ProxyProtocolConfig(ProxyProtocolMode.REQUIRED)))) {

            var address = tester.getBootstrapAddress();
            var parts = address.split(":");
            var host = parts[0];
            var port = Integer.parseInt(parts[1]);

            var group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
            try {
                var closeFuture = new java.util.concurrent.CompletableFuture<Void>();

                var bootstrap = new Bootstrap()
                        .group(group)
                        .channel(NioSocketChannel.class)
                        .handler(new ChannelInitializer<SocketChannel>() {
                            @Override
                            protected void initChannel(SocketChannel ch) {
                                ch.pipeline().addLast(new SimpleChannelInboundHandler<Object>() {
                                    @Override
                                    protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
                                    }

                                    @Override
                                    public void channelInactive(ChannelHandlerContext ctx) {
                                        closeFuture.complete(null);
                                    }
                                });
                            }
                        });

                Channel channel = bootstrap.connect(host, port).sync().channel();

                // Send garbage bytes (not a PROXY header)
                channel.writeAndFlush(Unpooled.wrappedBuffer("not a proxy header!!!".getBytes())).sync();

                // Then - connection should be closed quickly, not hang
                assertThat(closeFuture)
                        .as("Connection should be closed quickly when non-PROXY bytes are sent in required mode")
                        .succeedsWithin(5, TimeUnit.SECONDS);
            }
            finally {
                group.shutdownGracefully(0, 1, TimeUnit.SECONDS).sync();
            }
        }
    }

    // ---- Helper ----

    private Channel connectAndSendProxyHeader(MultiThreadIoEventLoopGroup group, String host, int port) throws Exception {
        var bootstrap = new Bootstrap()
                .group(group)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline().addLast(HAProxyMessageEncoder.INSTANCE);
                    }
                });

        Channel channel = bootstrap.connect(host, port).sync().channel();

        channel.writeAndFlush(new HAProxyMessage(
                HAProxyProtocolVersion.V2,
                HAProxyCommand.PROXY,
                HAProxyProxiedProtocol.TCP4,
                "192.168.1.100",
                "127.0.0.1",
                54321,
                port)).sync();

        return channel;
    }
}
