/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
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
            givenMockRespondsWithDowngradedV0ApiVersionsResponse(tester, ApiKeys.API_VERSIONS, ApiKeys.API_VERSIONS.oldestVersion(), brokerMaxVersion);
            Response response = whenGetApiVersionsFromKroxylicious(client);
            ByteBufferAccessor writable = new ByteBufferAccessor(ByteBuffer.allocate(20));
            // we have to compare bytes directly as our test code is relying on client code that will leniently
            // decode a v0 ApiVersionsResponse if there is some problem reading it with the latest version. We want
            // to be very sure that the bytes are what we hoped they were, so we compare directly.
            byte[] bytes = expectedKip511DowngradedApiVersionResponseHeaderAndBodyBytes(brokerMaxVersion, response, writable);
            assertThat(response.rawHeaderAndBodyBytes())
                    .describedAs("unexpected response header and body bytes").isEqualTo(bytes);
            assertKroxyliciousResponseOffersApiVersionsForApiKey(response, ApiKeys.API_VERSIONS, ApiKeys.API_VERSIONS.oldestVersion(),
                    brokerMaxVersion);
        }
    }

    private static byte[] expectedKip511DowngradedApiVersionResponseHeaderAndBodyBytes(short brokerMaxVersion, Response response, ByteBufferAccessor writable) {
        ApiVersionsResponseData apiVersionsRequestData = new ApiVersionsResponseData();
        ApiVersionsResponseData.ApiVersionCollection coll = new ApiVersionsResponseData.ApiVersionCollection();
        coll.add(new ApiVersionsResponseData.ApiVersion().setApiKey(ApiKeys.API_VERSIONS.id).setMinVersion(ApiKeys.API_VERSIONS.oldestVersion())
                .setMaxVersion(brokerMaxVersion));
        apiVersionsRequestData.setApiKeys(coll);
        apiVersionsRequestData.setErrorCode(Errors.UNSUPPORTED_VERSION.code());
        ObjectSerializationCache cache = new ObjectSerializationCache();
        short responseApiVersion = (short) 0;
        ResponseHeaderData responseHeaderData = new ResponseHeaderData().setCorrelationId(response.correlationId());
        responseHeaderData.write(writable, cache, ApiKeys.API_VERSIONS.responseHeaderVersion(responseApiVersion));
        apiVersionsRequestData.write(writable, cache, responseApiVersion);
        writable.flip();
        return writable.readArray(writable.remaining());
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
            givennMockRespondsWithApiVersionsForKey((short) 9999, (short) 3, (short) 4, tester, (short) 3);
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
        givennMockRespondsWithApiVersionsForKey(keys.id, minVersion, maxVersion, tester, (short) 3);
    }

    // if the upstream cannot handle the ApiVersions request, it responds with a v0 response with error code 35 as per KIP-511
    private static void givenMockRespondsWithDowngradedV0ApiVersionsResponse(MockServerKroxyliciousTester tester, ApiKeys keys, short minVersion, short maxVersion) {
        short apiVersion = (short) 0;
        ApiVersionsResponseData apiVersionsResponseData = givennMockRespondsWithApiVersionsForKey(keys.id, minVersion, maxVersion, tester, apiVersion);
        apiVersionsResponseData.setErrorCode(Errors.UNSUPPORTED_VERSION.code());
    }

    private static ApiVersionsResponseData givennMockRespondsWithApiVersionsForKey(short keys, short minVersion, short maxVersion, MockServerKroxyliciousTester tester,
                                                                                   short apiVersion) {
        ApiVersionsResponseData mockResponse = new ApiVersionsResponseData();
        ApiVersionsResponseData.ApiVersion version = new ApiVersionsResponseData.ApiVersion();
        version.setApiKey(keys).setMinVersion(minVersion).setMaxVersion(maxVersion);
        mockResponse.apiKeys().add(version);
        tester.addMockResponseForApiKey(new ResponsePayload(ApiKeys.API_VERSIONS, apiVersion, mockResponse));
        return mockResponse;
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
