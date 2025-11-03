/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy;

import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.github.nettyplus.leakdetector.junit.NettyLeakDetectorExtension;

import io.kroxylicious.proxy.internal.config.Feature;
import io.kroxylicious.proxy.internal.config.Features;
import io.kroxylicious.test.Request;
import io.kroxylicious.test.Response;
import io.kroxylicious.test.ResponsePayload;
import io.kroxylicious.test.client.KafkaClient;
import io.kroxylicious.test.tester.KroxyliciousConfigUtils;
import io.kroxylicious.test.tester.MockServerKroxyliciousTester;

import static io.kroxylicious.test.tester.KroxyliciousTesters.mockKafkaKroxyliciousTester;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * For the kafka RPCs that Kroxylicious can intercept, Kroxylicious should only offer API versions
 * that it can encode/decode using its version of the kafka-clients lib. It should also only offer API versions
 * that the Cluster can understand. So we use a Filter to intercept the ApiVersions response from the
 * Cluster and intersect those versions with the versions supported by the proxy. We offer the highest
 * minimum version for each api key supported by the Cluster and proxy. We offer the lowest maximum version
 * for each api key supported by the Cluster and proxy.
 * Any versions for ApiKeys unknown to the proxy are removed from the response. (ie the broker supports
 * a new ApiKey that this version of Kroxylicious is unaware of).
 */
@ExtendWith(NettyLeakDetectorExtension.class)
class ApiVersionsIT {

    @Test
    void shouldOfferTheMinimumHighestSupportedVersionWhenBrokerIsAheadOfKroxylicious() {
        try (var tester = mockKafkaKroxyliciousTester(KroxyliciousConfigUtils::proxy);
                var client = tester.simpleTestClient()) {
            givenMockRespondsWithApiVersionsForMetadataRequest(tester, ApiKeys.METADATA.oldestVersion(), (short) (ApiKeys.METADATA.latestVersion() + 1));
            Response response = whenGetApiVersionsFromKroxylicious(client);
            assertKroxyliciousResponseOffersApiVersionsForApiKey(response, ApiKeys.METADATA, ApiKeys.METADATA.oldestVersion(), ApiKeys.METADATA.latestVersion(),
                    Errors.NONE.code());
        }
    }

    @Test
    void shouldOfferTheMinimumHighestSupportedVersionWhenBrokerIsAheadOfKroxyliciousAndMaxVersionOverridden() {
        short overriddenVersion = (short) (ApiKeys.METADATA.latestVersion(true) - 1);
        Features testOnlyConfigEnabled = Features.builder().enable(Feature.TEST_ONLY_CONFIGURATION).build();
        try (var tester = mockKafkaKroxyliciousTester(bootstrap -> KroxyliciousConfigUtils.proxy(bootstrap)
                .withDevelopment(Map.of("apiKeyIdMaxVersionOverride", Map.of(ApiKeys.METADATA.name(), overriddenVersion))), testOnlyConfigEnabled);
                var client = tester.simpleTestClient()) {
            givenMockRespondsWithApiVersionsForMetadataRequest(tester, ApiKeys.METADATA.oldestVersion(), ApiKeys.METADATA.latestVersion(true));
            Response response = whenGetApiVersionsFromKroxylicious(client);
            assertKroxyliciousResponseOffersApiVersionsForApiKey(response, ApiKeys.METADATA, ApiKeys.METADATA.oldestVersion(), overriddenVersion, Errors.NONE.code());
        }
    }

    @Test
    void shouldOfferTheMinimumHighestSupportedVersionWhenKroxyliciousIsAheadOfBroker() {
        try (var tester = mockKafkaKroxyliciousTester(KroxyliciousConfigUtils::proxy);
                var client = tester.simpleTestClient()) {
            short expectedApiVersion = (short) (ApiKeys.METADATA.latestVersion() - 1);
            givenMockRespondsWithApiVersionsForMetadataRequest(tester, ApiKeys.METADATA.oldestVersion(), expectedApiVersion);
            Response response = whenGetApiVersionsFromKroxylicious(client);
            assertKroxyliciousResponseOffersApiVersionsForApiKey(response, ApiKeys.METADATA, ApiKeys.METADATA.oldestVersion(),
                    expectedApiVersion, Errors.NONE.code());
        }
    }

    /**
     * By the Kafka protocol, when the client ApiVersions request is ahead of the upstream, the server will respond
     * with a v0 response with error code 35 as per KIP-511. If the upstream responds this way, the proxy should
     * forward v0 response body bytes to the client.
     */
    @Test
    void shouldHandleVersionZeroErrorResponseWhenKroxyliciousIsAheadOfBroker() {
        try (var tester = mockKafkaKroxyliciousTester(KroxyliciousConfigUtils::proxy);
                var client = tester.simpleTestClient()) {
            short brokerMaxVersion = (short) (ApiKeys.API_VERSIONS.latestVersion() - 1);
            givenMockRespondsWithDowngradedV0ApiVersionsResponse(tester, ApiKeys.API_VERSIONS.oldestVersion(), brokerMaxVersion);
            Response response = whenGetApiVersionsFromKroxylicious(client, (short) 0);

            assertKroxyliciousResponseOffersApiVersionsForApiKey(response, ApiKeys.API_VERSIONS, ApiKeys.API_VERSIONS.oldestVersion(),
                    brokerMaxVersion, Errors.UNSUPPORTED_VERSION.code());
        }
    }

    @Test
    void shouldOfferTheMaximumLowestSupportedVersionWhenBrokerIsAheadOfKroxylicious() {
        try (var tester = mockKafkaKroxyliciousTester(KroxyliciousConfigUtils::proxy);
                var client = tester.simpleTestClient()) {
            short brokerOldestVersion = (short) (ApiKeys.METADATA.oldestVersion() + 1);
            givenMockRespondsWithApiVersionsForMetadataRequest(tester, brokerOldestVersion, ApiKeys.METADATA.latestVersion());
            Response response = whenGetApiVersionsFromKroxylicious(client);
            assertKroxyliciousResponseOffersApiVersionsForApiKey(response, ApiKeys.METADATA, brokerOldestVersion, ApiKeys.METADATA.latestVersion(), Errors.NONE.code());
        }
    }

    @Test
    void shouldOfferTheMaximumLowestSupportedVersionWhenKroxyliciousIsAheadOfBroker() {
        try (var tester = mockKafkaKroxyliciousTester(KroxyliciousConfigUtils::proxy);
                var client = tester.simpleTestClient()) {
            short brokerOldestVersion = (short) (ApiKeys.METADATA.oldestVersion() - 1);
            givenMockRespondsWithApiVersionsForMetadataRequest(tester, brokerOldestVersion, ApiKeys.METADATA.latestVersion());
            Response response = whenGetApiVersionsFromKroxylicious(client);
            assertKroxyliciousResponseOffersApiVersionsForApiKey(response, ApiKeys.METADATA, ApiKeys.METADATA.oldestVersion(), ApiKeys.METADATA.latestVersion(),
                    Errors.NONE.code());
        }
    }

    @Test
    void shouldNotOfferBrokerApisThatAreUnknownToKroxy() {
        try (var tester = mockKafkaKroxyliciousTester(KroxyliciousConfigUtils::proxy);
                var client = tester.simpleTestClient()) {
            ApiVersionsResponseData mockResponse = new ApiVersionsResponseData();
            ApiVersionsResponseData.ApiVersion version = new ApiVersionsResponseData.ApiVersion();
            version.setApiKey((short) 9999).setMinVersion((short) 3).setMaxVersion((short) 4);
            mockResponse.apiKeys().add(version);
            tester.addMockResponseForApiKey(new ResponsePayload(ApiKeys.API_VERSIONS, (short) 3, mockResponse));
            Response response = whenGetApiVersionsFromKroxylicious(client);
            ResponsePayload payload = response.payload();
            assertEquals(ApiKeys.API_VERSIONS, payload.apiKeys());
            assertEquals((short) 3, payload.apiVersion());
            ApiVersionsResponseData message = (ApiVersionsResponseData) payload.message();
            assertTrue(message.apiKeys().isEmpty());
        }
    }

    @Test
    void shouldOfferBrokerApisThatAreKnownToKroxy() {
        try (var tester = mockKafkaKroxyliciousTester(KroxyliciousConfigUtils::proxy);
                var client = tester.simpleTestClient()) {
            ApiVersionsResponseData mockResponse = new ApiVersionsResponseData();
            for (ApiKeys knownValue : ApiKeys.values()) {
                ApiVersionsResponseData.ApiVersion version = new ApiVersionsResponseData.ApiVersion();
                version.setApiKey(knownValue.id).setMinVersion(knownValue.oldestVersion()).setMaxVersion(knownValue.latestVersion());
                mockResponse.apiKeys().add(version);
            }
            tester.addMockResponseForApiKey(new ResponsePayload(ApiKeys.API_VERSIONS, (short) 3, mockResponse));
            Response response = whenGetApiVersionsFromKroxylicious(client);
            ResponsePayload payload = response.payload();
            assertEquals(ApiKeys.API_VERSIONS, payload.apiKeys());
            assertEquals((short) 3, payload.apiVersion());
            ApiVersionsResponseData message = (ApiVersionsResponseData) payload.message();
            Map<ApiKeys, ApiVersionsResponseData.ApiVersion> responseVersions = message.apiKeys().stream()
                    .collect(Collectors.toMap(k -> ApiKeys.forId(k.apiKey()), k -> k));
            for (ApiKeys knownValue : ApiKeys.values()) {
                assertTrue(responseVersions.containsKey(knownValue));
                assertEquals(knownValue.oldestVersion(), responseVersions.get(knownValue).minVersion());
                assertEquals(knownValue.latestVersion(), responseVersions.get(knownValue).maxVersion());
            }
        }
    }

    // Kafka 4 removed support for ProduceRequest v0..v2 however there is an issue with libRDKafka versions <= XXX that meant this broke compression support
    // https://issues.apache.org/jira/browse/KAFKA-18659 marks v0..v2 as supported versions however the broker will reject all uses of these old requests.
    // The proxy needs to replicate this special case handling so we can proxy older libRDKafka based clients (just about anything that isn't Java)
    @Test
    void shouldMarkProduceV0toV2AsSupportedVersions() {
        // Given
        try (var tester = mockKafkaKroxyliciousTester(KroxyliciousConfigUtils::proxy);
                var client = tester.simpleTestClient()) {
            ApiVersionsResponseData mockResponse = new ApiVersionsResponseData();
            ApiVersionsResponseData.ApiVersion version = new ApiVersionsResponseData.ApiVersion();
            version.setApiKey(ApiKeys.PRODUCE.id)
                    .setMinVersion(ApiKeys.PRODUCE_API_VERSIONS_RESPONSE_MIN_VERSION)
                    .setMaxVersion(ApiKeys.PRODUCE.latestVersion());
            mockResponse.apiKeys().add(version);
            tester.addMockResponseForApiKey(new ResponsePayload(ApiKeys.API_VERSIONS, (short) 3, mockResponse));

            // When
            Response response = whenGetApiVersionsFromKroxylicious(client);

            // Then
            ResponsePayload payload = response.payload();
            assertThat(payload.message())
                    .asInstanceOf(InstanceOfAssertFactories.type(ApiVersionsResponseData.class))
                    .satisfies(apiVersionsResponseData -> assertThat(apiVersionsResponseData.apiKeys())
                            .singleElement()
                            .satisfies(apiKeys -> assertThat(apiKeys)
                                    .satisfies(apiVersion -> {
                                        assertThat(apiVersion.apiKey()).isEqualTo(ApiKeys.PRODUCE.id);
                                        assertThat(apiVersion.minVersion()).isEqualTo(ApiKeys.PRODUCE_API_VERSIONS_RESPONSE_MIN_VERSION);
                                    })));
        }
    }

    private static void givenMockRespondsWithApiVersionsForMetadataRequest(MockServerKroxyliciousTester tester, short minVersion, short maxVersion) {
        ApiVersionsResponseData mockResponse = new ApiVersionsResponseData();
        ApiVersionsResponseData.ApiVersion version = new ApiVersionsResponseData.ApiVersion();
        version.setApiKey(ApiKeys.METADATA.id).setMinVersion(minVersion).setMaxVersion(maxVersion);
        mockResponse.apiKeys().add(version);
        tester.addMockResponseForApiKey(new ResponsePayload(ApiKeys.API_VERSIONS, (short) 3, mockResponse));
    }

    private static void givenMockRespondsWithDowngradedV0ApiVersionsResponse(MockServerKroxyliciousTester tester, short minVersion, short maxVersion) {
        ApiVersionsResponseData mockResponse = new ApiVersionsResponseData();
        ApiVersionsResponseData.ApiVersion version = new ApiVersionsResponseData.ApiVersion();
        version.setApiKey(ApiKeys.API_VERSIONS.id).setMinVersion(minVersion).setMaxVersion(maxVersion);
        mockResponse.apiKeys().add(version);
        mockResponse.setErrorCode(Errors.UNSUPPORTED_VERSION.code());
        tester.addMockResponseForApiKey(new ResponsePayload(ApiKeys.API_VERSIONS, ApiKeys.PRODUCE_API_VERSIONS_RESPONSE_MIN_VERSION, mockResponse));
    }

    private static Response whenGetApiVersionsFromKroxylicious(KafkaClient client) {
        return whenGetApiVersionsFromKroxylicious(client, (short) 3);
    }

    private static Response whenGetApiVersionsFromKroxylicious(KafkaClient client, short responseApiVersion) {
        return client.getSync(new Request(ApiKeys.API_VERSIONS, (short) 3, "client", new ApiVersionsRequestData(), responseApiVersion));
    }

    private static void assertKroxyliciousResponseOffersApiVersionsForApiKey(Response response, ApiKeys apiKeys, short minVersion, short maxVersion, short expected) {
        ResponsePayload payload = response.payload();
        assertEquals(ApiKeys.API_VERSIONS, payload.apiKeys());
        ApiVersionsResponseData message = (ApiVersionsResponseData) payload.message();
        assertThat(message.errorCode()).isEqualTo(expected);
        assertThat(message.apiKeys())
                .singleElement()
                .satisfies(apiVersion -> {
                    assertThat(apiVersion.apiKey()).isEqualTo(apiKeys.id);
                    assertThat(apiVersion.minVersion()).isEqualTo(minVersion);
                    assertThat(apiVersion.maxVersion()).isEqualTo(maxVersion);
                });
    }

}
