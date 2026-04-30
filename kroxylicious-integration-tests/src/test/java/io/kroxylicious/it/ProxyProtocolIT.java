/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.it;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.github.nettyplus.leakdetector.junit.NettyLeakDetectorExtension;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.haproxy.HAProxyCommand;
import io.netty.handler.codec.haproxy.HAProxyMessage;
import io.netty.handler.codec.haproxy.HAProxyMessageEncoder;
import io.netty.handler.codec.haproxy.HAProxyProtocolVersion;
import io.netty.handler.codec.haproxy.HAProxyProxiedProtocol;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;

import io.kroxylicious.proxy.config.ConfigurationBuilder;
import io.kroxylicious.proxy.config.ProxyProtocolConfig;
import io.kroxylicious.proxy.config.ProxyProtocolMode;
import io.kroxylicious.proxy.config.VirtualClusterBuilder;
import io.kroxylicious.test.Request;
import io.kroxylicious.test.Response;
import io.kroxylicious.test.ResponsePayload;
import io.kroxylicious.test.certificate.CertificateGenerator;
import io.kroxylicious.test.client.CorrelationManager;
import io.kroxylicious.test.client.KafkaClientHandler;
import io.kroxylicious.test.codec.KafkaRequestEncoder;
import io.kroxylicious.test.codec.KafkaResponseDecoder;
import io.kroxylicious.test.tester.KroxyliciousConfigUtils;

import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.DEFAULT_PROXY_BOOTSTRAP;
import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.defaultPortIdentifiesNodeGatewayBuilder;
import static io.kroxylicious.test.tester.KroxyliciousTesters.mockKafkaKroxyliciousTester;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for HaProxy PROXY protocol support.
 */
@ExtendWith(NettyLeakDetectorExtension.class)
class ProxyProtocolIT {

    @Test
    void requiredModeShouldRejectConnectionWithoutProxyHeader() {
        try (var tester = mockKafkaKroxyliciousTester(bootstrap -> KroxyliciousConfigUtils.proxy(bootstrap)
                .withProxyProtocol(new ProxyProtocolConfig(ProxyProtocolMode.REQUIRED)));
                var client = tester.simpleTestClient()) {

            var request = new Request(ApiKeys.API_VERSIONS, ApiVersionsRequestData.HIGHEST_SUPPORTED_VERSION,
                    "test-client", new ApiVersionsRequestData());

            // Connection should be rejected fast, not hang
            assertThat(client.get(request))
                    .failsWithin(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void requiredModeShouldProxyKafkaRequestsAfterProxyHeader() throws Exception {
        try (var tester = mockKafkaKroxyliciousTester(bootstrap -> KroxyliciousConfigUtils.proxy(bootstrap)
                .withProxyProtocol(new ProxyProtocolConfig(ProxyProtocolMode.REQUIRED)))) {

            tester.addMockResponseForApiKey(new ResponsePayload(ApiKeys.API_VERSIONS,
                    ApiVersionsRequestData.HIGHEST_SUPPORTED_VERSION, new ApiVersionsResponseData()));
            tester.addMockResponseForApiKey(new ResponsePayload(ApiKeys.METADATA,
                    MetadataRequestData.HIGHEST_SUPPORTED_VERSION, new MetadataResponseData()));

            var address = tester.getBootstrapAddress();
            var parts = address.split(":");
            var host = parts[0];
            var port = Integer.parseInt(parts[1]);

            var correlationManager = new CorrelationManager();
            var kafkaHandler = new KafkaClientHandler();
            try (var group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory())) {
                Channel channel = connectClient(group, host, port, correlationManager, kafkaHandler);

                // Send PROXY header first
                channel.writeAndFlush(new HAProxyMessage(
                        HAProxyProtocolVersion.V2,
                        HAProxyCommand.PROXY,
                        HAProxyProxiedProtocol.TCP4,
                        "192.168.1.100",
                        "127.0.0.1",
                        54321,
                        port)).sync();

                // Remove encoder after sending (PROXY header is one-shot)
                channel.pipeline().remove(HAProxyMessageEncoder.class);

                // Send ApiVersions request
                var apiVersionsRequest = toRequestFrame(new Request(ApiKeys.API_VERSIONS,
                        ApiVersionsRequestData.HIGHEST_SUPPORTED_VERSION, "test-client", new ApiVersionsRequestData()));
                CompletableFuture<?> apiVersionsFuture = kafkaHandler.sendRequest(apiVersionsRequest);
                assertThat(apiVersionsFuture)
                        .as("ApiVersions request should succeed after PROXY header")
                        .succeedsWithin(5, TimeUnit.SECONDS);

                // Send Metadata request on the same connection
                var metadataRequest = toRequestFrame(new Request(ApiKeys.METADATA,
                        MetadataRequestData.HIGHEST_SUPPORTED_VERSION, "test-client", new MetadataRequestData()));
                CompletableFuture<?> metadataFuture = kafkaHandler.sendRequest(metadataRequest);
                assertThat(metadataFuture)
                        .as("Metadata request should succeed after PROXY header")
                        .succeedsWithin(5, TimeUnit.SECONDS);

                channel.close().sync();
            }
        }
    }

    @Test
    void allowedModeShouldAcceptDirectConnectionWithoutProxyHeader() {
        try (var tester = mockKafkaKroxyliciousTester(bootstrap -> KroxyliciousConfigUtils.proxy(bootstrap)
                .withProxyProtocol(new ProxyProtocolConfig(ProxyProtocolMode.ALLOWED)));
                var client = tester.simpleTestClient()) {

            tester.addMockResponseForApiKey(new ResponsePayload(ApiKeys.API_VERSIONS,
                    ApiVersionsRequestData.HIGHEST_SUPPORTED_VERSION, new ApiVersionsResponseData()));

            var request = new Request(ApiKeys.API_VERSIONS, ApiVersionsRequestData.HIGHEST_SUPPORTED_VERSION,
                    "test-client", new ApiVersionsRequestData());

            Response response = client.getSync(request);
            assertThat(response.payload().message()).isInstanceOf(ApiVersionsResponseData.class);
        }
    }

    // ---- TLS + PROXY protocol tests ----

    @Test
    void tlsRequiredModeShouldProxyKafkaRequestsAfterProxyHeader() throws Exception {
        var keys = CertificateGenerator.generate();
        var keystore = keys.jksServerKeystore();

        try (var tester = mockKafkaKroxyliciousTester(bootstrap -> buildTlsProxyProtocolConfig(bootstrap, keystore, ProxyProtocolMode.REQUIRED))) {

            tester.addMockResponseForApiKey(new ResponsePayload(ApiKeys.API_VERSIONS,
                    ApiVersionsRequestData.HIGHEST_SUPPORTED_VERSION, new ApiVersionsResponseData()));

            var address = tester.getBootstrapAddress();
            var parts = address.split(":");
            var host = parts[0];
            var port = Integer.parseInt(parts[1]);

            var correlationManager = new CorrelationManager();
            var kafkaHandler = new KafkaClientHandler();
            SslContext sslContext = SslContextBuilder.forClient()
                    .trustManager(InsecureTrustManagerFactory.INSTANCE)
                    .build();

            try (var group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory())) {
                Channel channel = connectClient(group, host, port, correlationManager, kafkaHandler);

                // Send PROXY header first (before TLS handshake)
                channel.writeAndFlush(new HAProxyMessage(
                        HAProxyProtocolVersion.V2,
                        HAProxyCommand.PROXY,
                        HAProxyProxiedProtocol.TCP4,
                        "192.168.1.100",
                        "127.0.0.1",
                        54321,
                        port)).sync();

                // Remove PROXY encoder (one-shot) and add TLS handler at the front
                channel.pipeline().remove(HAProxyMessageEncoder.class);
                SslHandler sslHandler = sslContext.newHandler(channel.alloc(), host, port);
                channel.pipeline().addFirst(sslHandler);

                // Wait for TLS handshake to complete
                sslHandler.handshakeFuture().sync();

                // Send Kafka request over TLS
                var apiVersionsRequest = toRequestFrame(new Request(ApiKeys.API_VERSIONS,
                        ApiVersionsRequestData.HIGHEST_SUPPORTED_VERSION, "test-client", new ApiVersionsRequestData()));
                CompletableFuture<?> apiVersionsFuture = kafkaHandler.sendRequest(apiVersionsRequest);
                assertThat(apiVersionsFuture)
                        .as("ApiVersions request should succeed after PROXY header + TLS handshake")
                        .succeedsWithin(5, TimeUnit.SECONDS);

                channel.close().sync();
            }
        }
    }

    @Test
    void tlsRequiredModeShouldRejectTlsConnectionWithoutProxyHeader() {
        var keys = CertificateGenerator.generate();
        var keystore = keys.jksServerKeystore();

        try (var tester = mockKafkaKroxyliciousTester(bootstrap -> buildTlsProxyProtocolConfig(bootstrap, keystore, ProxyProtocolMode.REQUIRED));
                var client = tester.simpleTestClient()) {

            var request = new Request(ApiKeys.API_VERSIONS, ApiVersionsRequestData.HIGHEST_SUPPORTED_VERSION,
                    "test-client", new ApiVersionsRequestData());

            // TLS ClientHello bytes won't match PROXY header, connection should be rejected
            assertThat(client.get(request))
                    .failsWithin(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void tlsAllowedModeShouldAcceptDirectTlsConnectionWithoutProxyHeader() {
        var keys = CertificateGenerator.generate();
        var keystore = keys.jksServerKeystore();

        try (var tester = mockKafkaKroxyliciousTester(bootstrap -> buildTlsProxyProtocolConfig(bootstrap, keystore, ProxyProtocolMode.ALLOWED));
                var client = tester.simpleTestClient()) {

            tester.addMockResponseForApiKey(new ResponsePayload(ApiKeys.API_VERSIONS,
                    ApiVersionsRequestData.HIGHEST_SUPPORTED_VERSION, new ApiVersionsResponseData()));

            var request = new Request(ApiKeys.API_VERSIONS, ApiVersionsRequestData.HIGHEST_SUPPORTED_VERSION,
                    "test-client", new ApiVersionsRequestData());

            Response response = client.getSync(request);
            assertThat(response.payload().message()).isInstanceOf(ApiVersionsResponseData.class);
        }
    }

    @Test
    void tlsAllowedModeShouldAcceptTlsConnectionWithProxyHeader() throws Exception {
        var keys = CertificateGenerator.generate();
        var keystore = keys.jksServerKeystore();

        try (var tester = mockKafkaKroxyliciousTester(bootstrap -> buildTlsProxyProtocolConfig(bootstrap, keystore, ProxyProtocolMode.ALLOWED))) {

            tester.addMockResponseForApiKey(new ResponsePayload(ApiKeys.API_VERSIONS,
                    ApiVersionsRequestData.HIGHEST_SUPPORTED_VERSION, new ApiVersionsResponseData()));

            var address = tester.getBootstrapAddress();
            var parts = address.split(":");
            var host = parts[0];
            var port = Integer.parseInt(parts[1]);

            var correlationManager = new CorrelationManager();
            var kafkaHandler = new KafkaClientHandler();
            SslContext sslContext = SslContextBuilder.forClient()
                    .trustManager(InsecureTrustManagerFactory.INSTANCE)
                    .build();

            try (var group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory())) {
                Channel channel = connectClient(group, host, port, correlationManager, kafkaHandler);

                // Send PROXY header first (before TLS handshake)
                channel.writeAndFlush(new HAProxyMessage(
                        HAProxyProtocolVersion.V2,
                        HAProxyCommand.PROXY,
                        HAProxyProxiedProtocol.TCP4,
                        "192.168.1.100",
                        "127.0.0.1",
                        54321,
                        port)).sync();

                // Remove PROXY encoder (one-shot) and add TLS handler at the front
                channel.pipeline().remove(HAProxyMessageEncoder.class);
                SslHandler sslHandler = sslContext.newHandler(channel.alloc(), host, port);
                channel.pipeline().addFirst(sslHandler);

                // Wait for TLS handshake to complete
                sslHandler.handshakeFuture().sync();

                // Send Kafka request over TLS
                var apiVersionsRequest = toRequestFrame(new Request(ApiKeys.API_VERSIONS,
                        ApiVersionsRequestData.HIGHEST_SUPPORTED_VERSION, "test-client", new ApiVersionsRequestData()));
                CompletableFuture<?> apiVersionsFuture = kafkaHandler.sendRequest(apiVersionsRequest);
                assertThat(apiVersionsFuture)
                        .as("ApiVersions request should succeed in ALLOWED mode with PROXY header + TLS")
                        .succeedsWithin(5, TimeUnit.SECONDS);

                channel.close().sync();
            }
        }
    }

    @Test
    void disabledModeShouldRejectConnectionWithProxyHeader() throws Exception {
        try (var tester = mockKafkaKroxyliciousTester(bootstrap -> KroxyliciousConfigUtils.proxy(bootstrap)
                .withProxyProtocol(new ProxyProtocolConfig(ProxyProtocolMode.DISABLED)))) {

            var address = tester.getBootstrapAddress();
            var parts = address.split(":");
            var host = parts[0];
            var port = Integer.parseInt(parts[1]);

            var correlationManager = new CorrelationManager();
            var kafkaHandler = new KafkaClientHandler();
            try (var group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory())) {
                Channel channel = connectClient(group, host, port, correlationManager, kafkaHandler);

                // Send PROXY header — in DISABLED mode, proxy treats this as Kafka data
                channel.writeAndFlush(new HAProxyMessage(
                        HAProxyProtocolVersion.V2,
                        HAProxyCommand.PROXY,
                        HAProxyProxiedProtocol.TCP4,
                        "192.168.1.100",
                        "127.0.0.1",
                        54321,
                        port)).sync();

                channel.pipeline().remove(HAProxyMessageEncoder.class);

                // Send a Kafka request — should fail because the PROXY header bytes
                // were interpreted as a malformed Kafka request
                var apiVersionsRequest = toRequestFrame(new Request(ApiKeys.API_VERSIONS,
                        ApiVersionsRequestData.HIGHEST_SUPPORTED_VERSION, "test-client", new ApiVersionsRequestData()));
                CompletableFuture<?> apiVersionsFuture = kafkaHandler.sendRequest(apiVersionsRequest);
                assertThat(apiVersionsFuture)
                        .as("Request should fail when PROXY header is sent to DISABLED proxy")
                        .failsWithin(5, TimeUnit.SECONDS);

                channel.close().sync();
            }
        }
    }

    // ---- Helpers ----

    /**
     * Build a {@link Bootstrap} with the standard test-client pipeline
     * (PROXY encoder, Kafka codec, {@code kafkaHandler}) and wire channel-close
     * to {@link CorrelationManager#onChannelClose()} so in-flight request futures
     * are completed when the peer drops the connection.
     */
    private static Channel connectClient(EventLoopGroup group, String host, int port,
                                         CorrelationManager correlationManager, KafkaClientHandler kafkaHandler)
            throws InterruptedException {
        var bootstrap = new Bootstrap()
                .group(group)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline().addLast(HAProxyMessageEncoder.INSTANCE);
                        ch.pipeline().addLast(new KafkaRequestEncoder(correlationManager));
                        ch.pipeline().addLast(new KafkaResponseDecoder(correlationManager));
                        ch.pipeline().addLast(kafkaHandler);
                    }
                });
        var channel = bootstrap.connect(host, port).sync().channel();
        channel.closeFuture().addListener(f -> correlationManager.onChannelClose());
        return channel;
    }

    private static ConfigurationBuilder buildTlsProxyProtocolConfig(String mockBootstrap, CertificateGenerator.KeyStore keystore,
                                                                    ProxyProtocolMode mode) {
        return KroxyliciousConfigUtils.baseConfigurationBuilder()
                .addToVirtualClusters(new VirtualClusterBuilder()
                        .withName("demo")
                        .withNewTargetCluster()
                        .withBootstrapServers(mockBootstrap)
                        .endTargetCluster()
                        .addToGateways(defaultPortIdentifiesNodeGatewayBuilder(DEFAULT_PROXY_BOOTSTRAP)
                                .withNewTls()
                                .withNewKeyStoreKey()
                                .withStoreFile(keystore.path().toString())
                                .withStoreType(keystore.type())
                                .withNewInlinePasswordStoreProvider(keystore.storePassword())
                                .withNewInlinePasswordKeyProvider(keystore.keyPassword())
                                .endKeyStoreKey()
                                .endTls()
                                .build())
                        .build())
                .withProxyProtocol(new ProxyProtocolConfig(mode));
    }

    private static io.kroxylicious.test.codec.DecodedRequestFrame<?> toRequestFrame(Request request) {
        var header = new org.apache.kafka.common.message.RequestHeaderData()
                .setRequestApiKey(request.apiKeys().id)
                .setRequestApiVersion(request.apiVersion())
                .setClientId(request.clientIdHeader())
                .setCorrelationId(0);
        return new io.kroxylicious.test.codec.DecodedRequestFrame<>(
                header.requestApiVersion(), header.correlationId(), header, request.message(), request.apiVersion());
    }
}
