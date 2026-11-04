/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.it;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.github.nettyplus.leakdetector.junit.NettyLeakDetectorExtension;
import io.netty.handler.codec.haproxy.HAProxyMessage;

import io.kroxylicious.proxy.config.ConfigurationBuilder;
import io.kroxylicious.proxy.config.ProxyProtocolMode;
import io.kroxylicious.proxy.service.HostPort;
import io.kroxylicious.testing.certificate.CertificateGenerator;
import io.kroxylicious.testing.integration.Request;
import io.kroxylicious.testing.integration.ResponsePayload;
import io.kroxylicious.testing.integration.client.KafkaClient;

import static io.kroxylicious.testing.integration.client.KafkaClient.TRUST_ALL_CLIENT_SSL_CONTEXT;
import static io.kroxylicious.testing.integration.tester.KroxyliciousConfigUtils.DEFAULT_CLUSTER_DEF_NAME;
import static io.kroxylicious.testing.integration.tester.KroxyliciousConfigUtils.DEFAULT_CLUSTER_TARGET;
import static io.kroxylicious.testing.integration.tester.KroxyliciousConfigUtils.OS_ASSIGNED_BOOTSTRAP;
import static io.kroxylicious.testing.integration.tester.KroxyliciousConfigUtils.baseConfigurationBuilder;
import static io.kroxylicious.testing.integration.tester.KroxyliciousConfigUtils.defaultPortIdentifiesNodeGatewayBuilder;
import static io.kroxylicious.testing.integration.tester.KroxyliciousTesters.mockKafkaKroxyliciousTester;
import static io.netty.handler.codec.haproxy.HAProxyCommand.LOCAL;
import static io.netty.handler.codec.haproxy.HAProxyCommand.PROXY;
import static io.netty.handler.codec.haproxy.HAProxyProtocolVersion.V1;
import static io.netty.handler.codec.haproxy.HAProxyProtocolVersion.V2;
import static io.netty.handler.codec.haproxy.HAProxyProxiedProtocol.TCP4;
import static io.netty.handler.codec.haproxy.HAProxyProxiedProtocol.TCP6;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for per-gateway HAProxy PROXY protocol support.
 * Covers both plain-channel and TLS-channel cases for REQUIRED and FORBIDDEN modes,
 * parameterized across PROXY v1, PROXY v2 PROXY command, and PROXY v2 LOCAL command.
 */
@ExtendWith(NettyLeakDetectorExtension.class)
class ProxyProtocolIT {

    private static final CertificateGenerator.Keys TLS_KEYS = CertificateGenerator.generate();

    /**
     * PROXY messages to exercise in "with header" tests. Each call produces a fresh stream
     * of new HAProxyMessage instances — HAProxyMessage is ref-counted and the encoder releases
     * it after writing, so instances must not be shared across test invocations.
     */
    static Stream<Arguments> proxyMessages() {
        return Stream.of(
                Arguments.argumentSet("v1-proxy-tcp4",
                        new HAProxyMessage(V1, PROXY, TCP4, "192.168.1.100", "127.0.0.1", 54321, 9092)),
                Arguments.argumentSet("v1-proxy-tcp6",
                        new HAProxyMessage(V1, PROXY, TCP6, "2001:db8::1", "2001:db8::2", 54321, 9092)),
                Arguments.argumentSet("v2-proxy-tcp4",
                        new HAProxyMessage(V2, PROXY, TCP4, "192.168.1.100", "127.0.0.1", 54321, 9092)),
                Arguments.argumentSet("v2-proxy-tcp6",
                        new HAProxyMessage(V2, PROXY, TCP6, "::1", "::1", 54321, 9092)),
                // LOCAL command is used for health-check / loopback traffic. It is still a valid
                // PROXY v2 frame: REQUIRED mode must accept it, FORBIDDEN mode must reject it.
                // Netty's public HAProxyMessage constructor does not support AF_UNSPEC addresses,
                // so we use TCP4+localhost which produces a valid LOCAL-command frame on the wire.
                Arguments.argumentSet("v2-local",
                        new HAProxyMessage(V2, LOCAL, TCP4, "127.0.0.1", "127.0.0.1", 0, 0)));
    }

    // ---- plain channel ----

    @ParameterizedTest
    @MethodSource("proxyMessages")
    void requiredMode_plainChannel_withProxyHeader_succeeds(HAProxyMessage msg) {
        // Given
        try (var tester = mockKafkaKroxyliciousTester(bootstrap -> plainConfig(bootstrap, ProxyProtocolMode.REQUIRED))) {
            tester.addMockResponseForApiKey(new ResponsePayload(ApiKeys.API_VERSIONS,
                    ApiVersionsRequestData.HIGHEST_SUPPORTED_VERSION, new ApiVersionsResponseData()));
            var bootstrap = HostPort.parse(tester.getBootstrapAddress());
            try (var client = KafkaClient.withProxyProtocol(bootstrap.host(), bootstrap.port(), msg)) {

                // When
                var response = client.get(apiVersionsRequest());

                // Then
                assertThat(response).succeedsWithin(5, TimeUnit.SECONDS);
            }
            tester.assertAllMockInteractionsInvoked();
        }
    }

    @Test
    void requiredMode_plainChannel_withoutProxyHeader_rejects() {
        // Given
        try (var tester = mockKafkaKroxyliciousTester(bootstrap -> plainConfig(bootstrap, ProxyProtocolMode.REQUIRED));
                var client = tester.simpleTestClient()) {

            // When
            var response = client.get(apiVersionsRequest());

            // Then
            assertThat(response).failsWithin(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void forbiddenMode_plainChannel_withoutProxyHeader_succeeds() {
        // Given
        try (var tester = mockKafkaKroxyliciousTester(bootstrap -> plainConfig(bootstrap, ProxyProtocolMode.FORBIDDEN));
                var client = tester.simpleTestClient()) {
            tester.addMockResponseForApiKey(new ResponsePayload(ApiKeys.API_VERSIONS,
                    ApiVersionsRequestData.HIGHEST_SUPPORTED_VERSION, new ApiVersionsResponseData()));

            // When
            var response = client.get(apiVersionsRequest());

            // Then
            assertThat(response).succeedsWithin(5, TimeUnit.SECONDS);
            tester.assertAllMockInteractionsInvoked();
        }
    }

    @ParameterizedTest
    @MethodSource("proxyMessages")
    void forbiddenMode_plainChannel_withProxyHeader_rejects(HAProxyMessage msg) {
        // Given
        try (var tester = mockKafkaKroxyliciousTester(bootstrap -> plainConfig(bootstrap, ProxyProtocolMode.FORBIDDEN))) {
            var bootstrap = HostPort.parse(tester.getBootstrapAddress());
            try (var client = KafkaClient.withProxyProtocol(bootstrap.host(), bootstrap.port(), msg)) {

                // When
                var response = client.get(apiVersionsRequest());

                // Then
                assertThat(response).failsWithin(5, TimeUnit.SECONDS);
            }
        }
    }

    // ---- TLS channel ----

    @ParameterizedTest
    @MethodSource("proxyMessages")
    void requiredMode_tlsChannel_withProxyHeader_succeeds(HAProxyMessage msg) {
        // Given
        try (var tester = mockKafkaKroxyliciousTester(bootstrap -> tlsConfig(bootstrap, ProxyProtocolMode.REQUIRED))) {
            tester.addMockResponseForApiKey(new ResponsePayload(ApiKeys.API_VERSIONS,
                    ApiVersionsRequestData.HIGHEST_SUPPORTED_VERSION, new ApiVersionsResponseData()));
            var bootstrap = HostPort.parse(tester.getBootstrapAddress());
            try (var client = KafkaClient.withProxyProtocol(bootstrap.host(), bootstrap.port(),
                    msg, TRUST_ALL_CLIENT_SSL_CONTEXT)) {

                // When
                var response = client.get(apiVersionsRequest());

                // Then
                assertThat(response).succeedsWithin(5, TimeUnit.SECONDS);
            }
            tester.assertAllMockInteractionsInvoked();
        }
    }

    @Test
    void requiredMode_tlsChannel_withoutProxyHeader_rejects() {
        // Given
        try (var tester = mockKafkaKroxyliciousTester(bootstrap -> tlsConfig(bootstrap, ProxyProtocolMode.REQUIRED));
                var client = tester.simpleTestClient(tester.getBootstrapAddress(), true)) {

            // When
            var response = client.get(apiVersionsRequest());

            // Then
            assertThat(response).failsWithin(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void forbiddenMode_tlsChannel_withoutProxyHeader_succeeds() {
        // Given
        try (var tester = mockKafkaKroxyliciousTester(bootstrap -> tlsConfig(bootstrap, ProxyProtocolMode.FORBIDDEN));
                var client = tester.simpleTestClient(tester.getBootstrapAddress(), true)) {
            tester.addMockResponseForApiKey(new ResponsePayload(ApiKeys.API_VERSIONS,
                    ApiVersionsRequestData.HIGHEST_SUPPORTED_VERSION, new ApiVersionsResponseData()));

            // When
            var response = client.get(apiVersionsRequest());

            // Then
            assertThat(response).succeedsWithin(5, TimeUnit.SECONDS);
            tester.assertAllMockInteractionsInvoked();
        }
    }

    @ParameterizedTest
    @MethodSource("proxyMessages")
    void forbiddenMode_tlsChannel_withProxyHeader_rejects(HAProxyMessage msg) {
        // Given
        try (var tester = mockKafkaKroxyliciousTester(bootstrap -> tlsConfig(bootstrap, ProxyProtocolMode.FORBIDDEN))) {
            var bootstrap = HostPort.parse(tester.getBootstrapAddress());
            try (var client = KafkaClient.withProxyProtocol(bootstrap.host(), bootstrap.port(),
                    msg, TRUST_ALL_CLIENT_SSL_CONTEXT)) {

                // When
                var response = client.get(apiVersionsRequest());

                // Then
                assertThat(response).failsWithin(5, TimeUnit.SECONDS);
            }
        }
    }

    // ---- helpers ----

    private static Request apiVersionsRequest() {
        return new Request(ApiKeys.API_VERSIONS, ApiVersionsRequestData.HIGHEST_SUPPORTED_VERSION,
                "test-client", new ApiVersionsRequestData());
    }

    private static ConfigurationBuilder plainConfig(String bootstrap, ProxyProtocolMode mode) {
        // @formatter:off
        return baseConfigurationBuilder()
                .addNewClusterDefinition()
                    .withName(DEFAULT_CLUSTER_DEF_NAME)
                    .withBootstrapServers(bootstrap)
                .endClusterDefinition()
                .addNewVirtualCluster()
                    .withName("demo")
                    .withTarget(DEFAULT_CLUSTER_TARGET)
                    .addToGateways(defaultPortIdentifiesNodeGatewayBuilder(OS_ASSIGNED_BOOTSTRAP)
                            .withNewProxyProtocol()
                                .withMode(mode)
                            .endProxyProtocol()
                            .build())
                .endVirtualCluster();
        // @formatter:on
    }

    private static ConfigurationBuilder tlsConfig(String bootstrap, ProxyProtocolMode mode) {
        var keystore = TLS_KEYS.jksServerKeystore();
        // @formatter:off
        return baseConfigurationBuilder()
                .addNewClusterDefinition()
                    .withName(DEFAULT_CLUSTER_DEF_NAME)
                    .withBootstrapServers(bootstrap)
                .endClusterDefinition()
                .addNewVirtualCluster()
                    .withName("demo")
                    .withTarget(DEFAULT_CLUSTER_TARGET)
                    .addToGateways(defaultPortIdentifiesNodeGatewayBuilder(OS_ASSIGNED_BOOTSTRAP)
                            .withNewProxyProtocol()
                                .withMode(mode)
                            .endProxyProtocol()
                            .withNewTls()
                                .withNewKeyStoreKey()
                                    .withStoreFile(keystore.path().toString())
                                    .withStoreType(keystore.type())
                                    .withNewInlinePasswordStoreProvider(Objects.requireNonNull(keystore.storePassword()))
                                    .withNewInlinePasswordKeyProvider(Objects.requireNonNull(keystore.keyPassword()))
                                .endKeyStoreKey()
                            .endTls()
                            .build())
                .endVirtualCluster();
        // @formatter:on
    }
}
