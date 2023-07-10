/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy;

import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.junit.jupiter.api.Test;

import io.kroxylicious.test.Request;
import io.kroxylicious.test.Response;
import io.kroxylicious.test.client.KafkaClient;
import io.kroxylicious.test.tester.KroxyliciousConfigUtils;
import io.kroxylicious.test.tester.MockServerKroxyliciousTester;

import static io.kroxylicious.test.tester.KroxyliciousTesters.mockKafkaKroxyliciousTester;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * For the kafka RPCs that Kroxylicious can intercept, Kroxylicious should only offer API versions
 * that it can encode/decode using it's version of the kafka-clients lib. It should also only offer API versions
 * that the Cluster can understand. So we use a Filter to intercept the ApiVersions response from the
 * Cluster and intersect those versions with the versions supported by the proxy. We offer the highest
 * minimum version for each api key supported by the Cluster and proxy. We offer the lowest maximum version
 * for each api key supported by the Cluster and proxy.
 * Any versions for ApiKeys unknown to the proxy are forwarded to the client untouched. (ie the broker supports
 * a new ApiKey that this version of Kroxylicious is unaware of)
 * TODO check if this is still sensible behaviour, potentially a RequestFilter would attempt to decode these and fail.
 */
public class ApiVersionsIT {

    @Test
    public void shouldOfferTheMinimumHighestSupportedVersionWhenBrokerIsAheadOfKroxylicious() {
        try (var tester = mockKafkaKroxyliciousTester(KroxyliciousConfigUtils::proxy);
                var client = tester.singleRequestClient()) {
            givenMockRespondsWithApiVersionsForApiKey(tester, ApiKeys.METADATA, ApiKeys.METADATA.oldestVersion(), (short) (ApiKeys.METADATA.latestVersion() + 1));
            Response response = whenGetApiVersionsFromKroxylicious(client);
            assertKroxyliciousResponseOffersApiVersionsForApiKey(response, ApiKeys.METADATA, ApiKeys.METADATA.oldestVersion(), ApiKeys.METADATA.latestVersion());
        }
    }

    @Test
    public void shouldOfferTheMinimumHighestSupportedVersionWhenKroxyliciousIsAheadOfBroker() {
        try (var tester = mockKafkaKroxyliciousTester(KroxyliciousConfigUtils::proxy);
                var client = tester.singleRequestClient()) {
            givenMockRespondsWithApiVersionsForApiKey(tester, ApiKeys.METADATA, ApiKeys.METADATA.oldestVersion(), (short) (ApiKeys.METADATA.latestVersion() - 1));
            Response response = whenGetApiVersionsFromKroxylicious(client);
            assertKroxyliciousResponseOffersApiVersionsForApiKey(response, ApiKeys.METADATA, ApiKeys.METADATA.oldestVersion(),
                    (short) (ApiKeys.METADATA.latestVersion() - 1));
        }
    }

    @Test
    public void shouldOfferTheMaximumLowestSupportedVersionWhenBrokerIsAheadOfKroxylicious() {
        try (var tester = mockKafkaKroxyliciousTester(KroxyliciousConfigUtils::proxy);
                var client = tester.singleRequestClient()) {
            short brokerOldestVersion = (short) (ApiKeys.METADATA.oldestVersion() + 1);
            givenMockRespondsWithApiVersionsForApiKey(tester, ApiKeys.METADATA, brokerOldestVersion, ApiKeys.METADATA.latestVersion());
            Response response = whenGetApiVersionsFromKroxylicious(client);
            assertKroxyliciousResponseOffersApiVersionsForApiKey(response, ApiKeys.METADATA, brokerOldestVersion, ApiKeys.METADATA.latestVersion());
        }
    }

    @Test
    public void shouldOfferTheMaximumLowestSupportedVersionWhenKroxyliciousIsAheadOfBroker() {
        try (var tester = mockKafkaKroxyliciousTester(KroxyliciousConfigUtils::proxy);
                var client = tester.singleRequestClient()) {
            short brokerOldestVersion = (short) (ApiKeys.METADATA.oldestVersion() - 1);
            givenMockRespondsWithApiVersionsForApiKey(tester, ApiKeys.METADATA, brokerOldestVersion, ApiKeys.METADATA.latestVersion());
            Response response = whenGetApiVersionsFromKroxylicious(client);
            assertKroxyliciousResponseOffersApiVersionsForApiKey(response, ApiKeys.METADATA, ApiKeys.METADATA.oldestVersion(), ApiKeys.METADATA.latestVersion());
        }
    }

    // TODO we think this is a bug but will come back to it
    @Test
    public void shouldOfferBrokerApisThatAreUnknownToKroxy() {
        try (var tester = mockKafkaKroxyliciousTester(KroxyliciousConfigUtils::proxy);
                var client = tester.singleRequestClient()) {
            ApiVersionsResponseData mockResponse = new ApiVersionsResponseData();
            ApiVersionsResponseData.ApiVersion version = new ApiVersionsResponseData.ApiVersion();
            version.setApiKey((short) 9999).setMinVersion((short) 3).setMaxVersion((short) 4);
            mockResponse.apiKeys().add(version);
            tester.setMockResponse(new Response(ApiKeys.API_VERSIONS, (short) 3, mockResponse));
            Response response = whenGetApiVersionsFromKroxylicious(client);
            assertEquals(ApiKeys.API_VERSIONS, response.apiKeys());
            assertEquals((short) 3, response.apiVersion());
            ApiVersionsResponseData message = (ApiVersionsResponseData) response.message();
            assertEquals(1, message.apiKeys().size());
            ApiVersionsResponseData.ApiVersion singletonVersion = message.apiKeys().iterator().next();
            assertEquals((short) 9999, singletonVersion.apiKey());
            assertEquals((short) 3, singletonVersion.minVersion());
            assertEquals((short) 4, singletonVersion.maxVersion());
        }
    }

    private static void givenMockRespondsWithApiVersionsForApiKey(MockServerKroxyliciousTester tester, ApiKeys keys, short minVersion, short maxVersion) {
        ApiVersionsResponseData mockResponse = new ApiVersionsResponseData();
        ApiVersionsResponseData.ApiVersion version = new ApiVersionsResponseData.ApiVersion();
        version.setApiKey(keys.id).setMinVersion(minVersion).setMaxVersion(maxVersion);
        mockResponse.apiKeys().add(version);
        tester.setMockResponse(new Response(ApiKeys.API_VERSIONS, (short) 3, mockResponse));
    }

    private static Response whenGetApiVersionsFromKroxylicious(KafkaClient client) {
        return client.getSync(new Request(ApiKeys.API_VERSIONS, (short) 3, "client", new ApiVersionsRequestData()));
    }

    private static void assertKroxyliciousResponseOffersApiVersionsForApiKey(Response response, ApiKeys apiKeys, short minVersion, short maxVersion) {
        assertEquals(ApiKeys.API_VERSIONS, response.apiKeys());
        assertEquals((short) 3, response.apiVersion());
        ApiVersionsResponseData message = (ApiVersionsResponseData) response.message();
        assertEquals(1, message.apiKeys().size());
        ApiVersionsResponseData.ApiVersion singletonVersion = message.apiKeys().iterator().next();
        assertEquals(apiKeys.id, singletonVersion.apiKey());
        assertEquals(minVersion, singletonVersion.minVersion());
        assertEquals(maxVersion, singletonVersion.maxVersion());
    }

}
