/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.NodeApiVersions;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.security.plain.PlainLoginModule;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.utility.DockerImageName;

import io.github.nettyplus.leakdetector.junit.NettyLeakDetectorExtension;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import io.kroxylicious.proxy.internal.config.Feature;
import io.kroxylicious.proxy.internal.config.Features;
import io.kroxylicious.test.Response;
import io.kroxylicious.test.codec.ByteBufAccessorImpl;
import io.kroxylicious.test.codec.OpaqueRequestFrame;
import io.kroxylicious.test.tester.KroxyliciousConfigUtils;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.common.SaslMechanism;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;

import edu.umd.cs.findbugs.annotations.NonNull;

import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.proxy;
import static io.kroxylicious.test.tester.KroxyliciousTesters.mockKafkaKroxyliciousTester;
import static io.kroxylicious.test.tester.KroxyliciousTesters.newBuilder;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * <p>
 * In addition to the behaviour tested by {@link ApiVersionsIT} we also need to handle the client
 * sending ApiVersions requests at an apiVersion higher than the proxy understands. With no proxy
 * involved, the behaviour is the broker will respond with a v0 apiversions response and the error
 * code is set to UNSUPPORTED_VERSION and the api_versions array populated with the supported versions
 * for ApiVersions, so that the client can retry with the highest supported ApiVersions version.
 * </p>
 * <br>
 * In this case the proxy will respond with its own v0 response populated with the proxy's supported
 * ApiVersions versions. It may be that on the following request the upstream broker will be behind
 * the proxy and respond with it's own UNSUPPORTED_VERSION response. So the clients must be able to
 * tolerate 2 UNSUPPORTED_VERSION responses in a row.
 * </p>
 */
@ExtendWith(NettyLeakDetectorExtension.class)
@ExtendWith(KafkaClusterExtension.class)
public class ApiVersionsDowngradeIT {

    public static final int CORRELATION_ID = 100;
    public static final short API_VERSIONS_ID = ApiKeys.API_VERSIONS.id;
    public static final String SASL_USER = "alice";
    public static final String SASL_PASSWORD = "foo";
    private static final DockerImageName OLD_REDPANDA_USING_API_VER0_2 = DockerImageName.parse("redpandadata/redpanda:v22.1.11");

    @Test
    void clientAheadOfProxy() {
        // Given
        try (var tester = mockKafkaKroxyliciousTester(KroxyliciousConfigUtils::proxy);
                var client = tester.simpleTestClient()) {
            OpaqueRequestFrame frame = createHypotheticalFutureRequest();
            ApiVersionsResponseData.ApiVersion expected = new ApiVersionsResponseData.ApiVersion();
            ApiVersionsResponseData.ApiVersionCollection collection = new ApiVersionsResponseData.ApiVersionCollection();
            expected.setApiKey(ApiKeys.API_VERSIONS.id).setMinVersion(ApiKeys.API_VERSIONS.oldestVersion())
                    .setMaxVersion(ApiKeys.API_VERSIONS.latestVersion(true));
            collection.add(expected);

            // When
            CompletableFuture<Response> responseCompletableFuture = client.get(frame);

            // Then
            assertThat(responseCompletableFuture).succeedsWithin(5, TimeUnit.SECONDS)
                    .satisfies(response -> {
                        assertThat(response.payload().apiKeys()).isEqualTo(ApiKeys.API_VERSIONS);
                        assertThat(response.payload().message()).isInstanceOfSatisfying(ApiVersionsResponseData.class,
                                data -> {
                                    assertThat(data.apiKey()).isEqualTo(ApiKeys.API_VERSIONS.id);
                                    assertThat(data.errorCode()).isEqualTo(Errors.UNSUPPORTED_VERSION.code());
                                    assertThat(data.apiKeys()).isEqualTo(collection);
                                });
                    });
            assertThat(tester.getReceivedRequestCount()).isZero();
        }
    }

    @Test
    void proxyRestrictedToOlderApiVersion(KafkaCluster cluster) {
        doProxyRestrictedToOlderApiVersion(cluster, Map.of());
    }

    @Test
    void proxyRestrictedToOlderApiVersionWithSasl(@SaslMechanism(principals = {
            @SaslMechanism.Principal(user = SASL_USER, password = SASL_PASSWORD) }) KafkaCluster cluster) {
        var clientSecurityProtocolConfig = new HashMap<String, Object>();
        clientSecurityProtocolConfig.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.name);
        clientSecurityProtocolConfig.put(SaslConfigs.SASL_JAAS_CONFIG,
                String.format("""
                        %s required username="%s" password="%s";""",
                        PlainLoginModule.class.getName(), SASL_USER, SASL_PASSWORD));
        clientSecurityProtocolConfig.put(SaslConfigs.SASL_MECHANISM, "PLAIN");

        doProxyRestrictedToOlderApiVersion(cluster, clientSecurityProtocolConfig);
    }

    private void doProxyRestrictedToOlderApiVersion(KafkaCluster cluster, Map<String, Object> clientSecurityProtocolConfig) {
        var apiVersion = (short) (ApiKeys.API_VERSIONS.latestVersion() - 1);
        var testConfigEnabled = Features.builder().enable(Feature.TEST_ONLY_CONFIGURATION).build();
        var proxy = proxy(cluster)
                .withDevelopment(Map.of("apiKeyIdMaxVersionOverride", Map.of(ApiKeys.API_VERSIONS.name(), apiVersion)));
        try (var tester = newBuilder(proxy).setFeatures(testConfigEnabled).createDefaultKroxyliciousTester();
                var admin = tester.admin(clientSecurityProtocolConfig)) {
            // We've got no way to observe the actual version of the API versions request that is used during _negotiation_
            // so we make do with asserting the connection is usable.
            final var result = admin.describeCluster().clusterId();
            assertThat(result).as("Unable to get the clusterId from the Kafka cluster").succeedsWithin(Duration.ofSeconds(10));
            // check that the client is actually using the correct version.
            assertThat(admin)
                    .extracting("instance")
                    .extracting("client")
                    .extracting("apiVersions")
                    .extracting("nodeApiVersions", InstanceOfAssertFactories.map(String.class, NodeApiVersions.class))
                    .hasEntrySatisfying("0", nav -> assertThat(nav.apiVersion(ApiKeys.API_VERSIONS).maxVersion())
                            .isEqualTo(apiVersion));
        }
    }

    /**
     * In this test, we verify the assumption that the Kafka Client is capable
     * of negotiating down api-versions version twice.  This test uses an
     * old version of Redpanda (that supports max v2) and the Proxy (restricted
     * to v3).  The Kafka Client is using v4.
     * <br>
     * Client will first make a v4 request.  The proxy's response will cause the client
     * to try again at v3.  The broker will then cause the client to try a third time at v2.
     * This request will satisfy all parties and the connection establishment will continue.
     * All this occurs on a single connection.
     */
    @Test
    @EnabledIf(value = "isDockerAvailable", disabledReason = "docker unavailable")
    void clientAheadOfProxyWhichIsAheadOfBroker() {

        try (var redpanda = createRedpanda()) {
            redpanda.start();
            var redpandaApiVersion = (short) 2;
            var proxyApiVersion = (short) (ApiKeys.API_VERSIONS.latestVersion() - 1);
            assertThat(redpandaApiVersion).isLessThan(proxyApiVersion);

            var testConfigEnabled = Features.builder().enable(Feature.TEST_ONLY_CONFIGURATION).build();
            var proxy = proxy("localhost:9092")
                    .withDevelopment(Map.of("apiKeyIdMaxVersionOverride", Map.of(ApiKeys.API_VERSIONS.name(), proxyApiVersion)));
            try (var tester = newBuilder(proxy).setFeatures(testConfigEnabled).createDefaultKroxyliciousTester();
                    var admin = tester.admin()) {
                // We've got no way to observe the actual version of the API versions request that is used during _negotiation_
                // so we make do with asserting the connection is usable.
                final var result = admin.describeCluster().clusterId();
                assertThat(result).as("Unable to get the clusterId from the Kafka cluster").succeedsWithin(Duration.ofSeconds(10));
                // check that the client is actually using the correct version.
                assertThat(admin)
                        .extracting("instance")
                        .extracting("client")
                        .extracting("apiVersions")
                        .extracting("nodeApiVersions", InstanceOfAssertFactories.map(String.class, NodeApiVersions.class))
                        .hasEntrySatisfying("-1", nav -> assertThat(nav.apiVersion(ApiKeys.API_VERSIONS).maxVersion())
                                .isEqualTo(redpandaApiVersion));
            }

        }
    }

    private static @NonNull OpaqueRequestFrame createHypotheticalFutureRequest() {
        short unsupportedVersion = (short) (ApiKeys.API_VERSIONS.latestVersion(true) + 1);
        RequestHeaderData requestHeaderData = getRequestHeaderData(unsupportedVersion);
        short requestHeaderVersion = ApiKeys.API_VERSIONS.requestHeaderVersion(unsupportedVersion);
        ObjectSerializationCache cache = new ObjectSerializationCache();
        int headerSize = requestHeaderData.size(cache, requestHeaderVersion);
        // the proxy can assume that it is safe to read the latest header version from the message, but any
        // bytes after than cannot be read because the future message may be using a new header version with
        // additional fields. So the bytes after the latest known header bytes are arbitrary as far as the proxy
        // is concerned.
        byte[] arbitraryBodyData = new byte[]{ 1, 2, 3, 4 };
        int bodySize = arbitraryBodyData.length;
        int messageSize = headerSize + bodySize;
        ByteBuf buffer = Unpooled.buffer(messageSize, messageSize);
        ByteBufAccessorImpl accessor = new ByteBufAccessorImpl(buffer);
        requestHeaderData.write(accessor, cache, requestHeaderVersion);
        accessor.writeByteArray(arbitraryBodyData);
        return new OpaqueRequestFrame(buffer, CORRELATION_ID, messageSize, true, ApiKeys.API_VERSIONS, unsupportedVersion, (short) 0);
    }

    private static @NonNull RequestHeaderData getRequestHeaderData(short unsupportedVersion) {
        RequestHeaderData requestHeaderData = new RequestHeaderData();
        requestHeaderData.setRequestApiKey(ApiVersionsDowngradeIT.API_VERSIONS_ID);
        requestHeaderData.setRequestApiVersion(unsupportedVersion);
        requestHeaderData.setCorrelationId(ApiVersionsDowngradeIT.CORRELATION_ID);
        return requestHeaderData;
    }

    @NonNull
    private RedpandaContainer createRedpanda() {
        var redpanda = new RedpandaContainer(ApiVersionsDowngradeIT.OLD_REDPANDA_USING_API_VER0_2);
        redpanda.setWaitStrategy(new LogMessageWaitStrategy().withRegEx(".*Bootstrap complete.*"));
        redpanda.addFixedExposedPort(9092, 9092);
        return redpanda;
    }

    private static class RedpandaContainer extends GenericContainer<RedpandaContainer> {

        private RedpandaContainer(DockerImageName dockerImageName) {
            super(dockerImageName);
        }

        @Override
        protected void addFixedExposedPort(int hostPort, int containerPort) {
            super.addFixedExposedPort(hostPort, containerPort);
        }
    }

    static boolean isDockerAvailable() {
        return DockerClientFactory.instance().isDockerAvailable();
    }
}
