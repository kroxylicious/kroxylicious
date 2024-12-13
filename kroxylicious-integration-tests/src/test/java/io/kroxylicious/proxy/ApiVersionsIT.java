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
public class ApiVersionsIT {

    @Test
    void shouldOfferTheMinimumHighestSupportedVersionWhenBrokerIsAheadOfKroxylicious() {
        try (var tester = mockKafkaKroxyliciousTester(KroxyliciousConfigUtils::proxy);
                var client = tester.simpleTestClient()) {
            givenMockRespondsWithApiVersionsForApiKey(tester, ApiKeys.METADATA, ApiKeys.METADATA.oldestVersion(), (short) (ApiKeys.METADATA.latestVersion() + 1));
            Response response = whenGetApiVersionsFromKroxylicious(client);
            assertKroxyliciousResponseOffersApiVersionsForApiKey(response, ApiKeys.METADATA, ApiKeys.METADATA.oldestVersion(), ApiKeys.METADATA.latestVersion());
        }
    }

    @Test
    void shouldOfferTheMinimumHighestSupportedVersionWhenBrokerIsAheadOfKroxyliciousAndMaxVersionOverridden() {
        short overriddenVersion = (short) (ApiKeys.METADATA.latestVersion(true) - 1);
        Features testOnlyConfigEnabled = Features.builder().enable(Feature.TEST_ONLY_CONFIGURATION).build();
        try (var tester = mockKafkaKroxyliciousTester(bootstrap -> KroxyliciousConfigUtils.proxy(bootstrap)
                .withDevelopment(Map.of("apiKeyIdMaxVersionOverride", Map.of(ApiKeys.METADATA.name(), overriddenVersion))), testOnlyConfigEnabled);
                var client = tester.simpleTestClient()) {
            givenMockRespondsWithApiVersionsForApiKey(tester, ApiKeys.METADATA, ApiKeys.METADATA.oldestVersion(), ApiKeys.METADATA.latestVersion(true));
            Response response = whenGetApiVersionsFromKroxylicious(client);
            assertKroxyliciousResponseOffersApiVersionsForApiKey(response, ApiKeys.METADATA, ApiKeys.METADATA.oldestVersion(), overriddenVersion);
        }
    }

    @Test
    void shouldOfferTheMinimumHighestSupportedVersionWhenKroxyliciousIsAheadOfBroker() {
        try (var tester = mockKafkaKroxyliciousTester(KroxyliciousConfigUtils::proxy);
                var client = tester.simpleTestClient()) {
            givenMockRespondsWithApiVersionsForApiKey(tester, ApiKeys.METADATA, ApiKeys.METADATA.oldestVersion(), (short) (ApiKeys.METADATA.latestVersion() - 1));
            Response response = whenGetApiVersionsFromKroxylicious(client);
            assertKroxyliciousResponseOffersApiVersionsForApiKey(response, ApiKeys.METADATA, ApiKeys.METADATA.oldestVersion(),
                    (short) (ApiKeys.METADATA.latestVersion() - 1));
        }
    }

    @Test
    void shouldOfferTheMaximumLowestSupportedVersionWhenBrokerIsAheadOfKroxylicious() {
        try (var tester = mockKafkaKroxyliciousTester(KroxyliciousConfigUtils::proxy);
                var client = tester.simpleTestClient()) {
            short brokerOldestVersion = (short) (ApiKeys.METADATA.oldestVersion() + 1);
            givenMockRespondsWithApiVersionsForApiKey(tester, ApiKeys.METADATA, brokerOldestVersion, ApiKeys.METADATA.latestVersion());
            Response response = whenGetApiVersionsFromKroxylicious(client);
            assertKroxyliciousResponseOffersApiVersionsForApiKey(response, ApiKeys.METADATA, brokerOldestVersion, ApiKeys.METADATA.latestVersion());
        }
    }

    @Test
    void shouldOfferTheMaximumLowestSupportedVersionWhenKroxyliciousIsAheadOfBroker() {
        try (var tester = mockKafkaKroxyliciousTester(KroxyliciousConfigUtils::proxy);
                var client = tester.simpleTestClient()) {
            short brokerOldestVersion = (short) (ApiKeys.METADATA.oldestVersion() - 1);
            givenMockRespondsWithApiVersionsForApiKey(tester, ApiKeys.METADATA, brokerOldestVersion, ApiKeys.METADATA.latestVersion());
            Response response = whenGetApiVersionsFromKroxylicious(client);
            assertKroxyliciousResponseOffersApiVersionsForApiKey(response, ApiKeys.METADATA, ApiKeys.METADATA.oldestVersion(), ApiKeys.METADATA.latestVersion());
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

    private static void givenMockRespondsWithApiVersionsForApiKey(MockServerKroxyliciousTester tester, ApiKeys keys, short minVersion, short maxVersion) {
        ApiVersionsResponseData mockResponse = new ApiVersionsResponseData();
        ApiVersionsResponseData.ApiVersion version = new ApiVersionsResponseData.ApiVersion();
        version.setApiKey(keys.id).setMinVersion(minVersion).setMaxVersion(maxVersion);
        mockResponse.apiKeys().add(version);
        tester.addMockResponseForApiKey(new ResponsePayload(ApiKeys.API_VERSIONS, (short) 3, mockResponse));
    }

    private static Response whenGetApiVersionsFromKroxylicious(KafkaClient client) {
        return client.getSync(new Request(ApiKeys.API_VERSIONS, (short) 3, "client", new ApiVersionsRequestData()));
    }

    private static void assertKroxyliciousResponseOffersApiVersionsForApiKey(Response response, ApiKeys apiKeys, short minVersion, short maxVersion) {
        ResponsePayload payload = response.payload();
        assertEquals(ApiKeys.API_VERSIONS, payload.apiKeys());
        assertEquals((short) 3, payload.apiVersion());
        ApiVersionsResponseData message = (ApiVersionsResponseData) payload.message();
        assertEquals(1, message.apiKeys().size());
        ApiVersionsResponseData.ApiVersion singletonVersion = message.apiKeys().iterator().next();
        assertEquals(apiKeys.id, singletonVersion.apiKey());
        assertEquals(minVersion, singletonVersion.minVersion());
        assertEquals(maxVersion, singletonVersion.maxVersion());
    }

}
