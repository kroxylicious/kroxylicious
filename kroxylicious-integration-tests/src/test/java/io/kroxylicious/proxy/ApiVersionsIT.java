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
import org.assertj.core.matcher.AssertionMatcher;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.github.nettyplus.leakdetector.junit.NettyLeakDetectorExtension;

import io.kroxylicious.test.Request;
import io.kroxylicious.test.Response;
import io.kroxylicious.test.ResponsePayload;
import io.kroxylicious.test.client.KafkaClient;
import io.kroxylicious.test.tester.KroxyliciousConfigUtils;
import io.kroxylicious.test.tester.MockServerKroxyliciousTester;

import edu.umd.cs.findbugs.annotations.NonNull;

import static io.kroxylicious.test.tester.KroxyliciousTesters.mockKafkaKroxyliciousTester;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

    @Test
    void clientUsesNewerApiVersionRpcThanProxyAndBroker() {
        try (var tester = mockKafkaKroxyliciousTester(KroxyliciousConfigUtils::proxy);
                var client = tester.simpleTestClient()) {

            var clientVersion = (short) (ApiKeys.API_VERSIONS.latestVersion() + 1);

            var unsupportedResponse = kip511UnsupportedApiVersionResponse();
            tester.addMockResponse(new AssertionMatcher<>() {
                @Override
                public void assertion(Request actual) throws AssertionError {
                    // expecting the request arriving at the broker to be serialised with the
                    // highest apiVersion used known to the proxy
                    assertThat(actual.apiVersion()).isEqualTo(ApiKeys.API_VERSIONS.latestVersion());
                }
            }, unsupportedResponse);

            var response = client.getSync(new Request(ApiKeys.API_VERSIONS, clientVersion, "client", new ApiVersionsRequestData()));
            assertThat(response.payload().message())
                    .asInstanceOf(InstanceOfAssertFactories.type(ApiVersionsResponseData.class))
                    .returns(Errors.UNSUPPORTED_VERSION.code(), ApiVersionsResponseData::errorCode)
                    .returns(unsupportedResponse.message().lowestSupportedVersion(), ApiVersionsResponseData::lowestSupportedVersion)
                    .returns(unsupportedResponse.message().highestSupportedVersion(), ApiVersionsResponseData::highestSupportedVersion)
            ;
        }
    }

    @Test
    void clientAndBrokerUsesNewerApiVersionThanProxy() {
        try (var tester = mockKafkaKroxyliciousTester(KroxyliciousConfigUtils::proxy);
                var client = tester.simpleTestClient()) {

            var clientVersion = (short) (ApiKeys.API_VERSIONS.latestVersion() + 1);

            var supportedResponse = supportedApiVersionResponse(ApiKeys.API_VERSIONS.latestVersion(), ApiKeys.API_VERSIONS.oldestVersion(), ApiKeys.API_VERSIONS.latestVersion());
            tester.addMockResponse(new AssertionMatcher<>() {
                @Override
                public void assertion(Request actual) throws AssertionError {
                    assertThat(actual.apiVersion()).isEqualTo(ApiKeys.API_VERSIONS.latestVersion());
                }
            }, supportedResponse);

            var response = client.getSync(new Request(ApiKeys.API_VERSIONS, clientVersion, "client", new ApiVersionsRequestData()));
            assertThat(response.payload().message())
                    .asInstanceOf(InstanceOfAssertFactories.type(ApiVersionsResponseData.class))
                    .returns(Errors.UNSUPPORTED_VERSION.code(), ApiVersionsResponseData::errorCode)
                    .returns(ApiKeys.API_VERSIONS.oldestVersion(), ApiVersionsResponseData::lowestSupportedVersion)
                    .returns(ApiKeys.API_VERSIONS.latestVersion(), ApiVersionsResponseData::highestSupportedVersion)
            ;
        }
    }

    /**
     * This is the response used by broker when it receives an ApiVersion request at a version
     * it does not understand.
     * It is defined by <a href="https://cwiki.apache.org/confluence/display/KAFKA/KIP-511%3A+Collect+and+Expose+Client%27s+Name+and+Version+in+the+Brokers#KIP511:CollectandExposeClient">KIP-511</a>'sNameandVersionintheBrokers-ApiVersionsRequest/ResponseHandling.1
     * @return unsupported api version response.
     */
    @NonNull
    private ResponsePayload kip511UnsupportedApiVersionResponse() {
        var unsupportedApiVersionResponse = new ApiVersionsResponseData();
        var apiVersions = ApiKeys.API_VERSIONS;
        var version = new ApiVersionsResponseData.ApiVersion();
        version.setApiKey(apiVersions.id).setMinVersion(apiVersions.oldestVersion()).setMaxVersion(apiVersions.latestVersion());
        unsupportedApiVersionResponse.apiKeys().add(version);
        unsupportedApiVersionResponse.setErrorCode(Errors.UNSUPPORTED_VERSION.code());
        return new ResponsePayload(ApiKeys.API_VERSIONS, (short) 0, unsupportedApiVersionResponse);
    }

    @NonNull
    private ResponsePayload supportedApiVersionResponse(short apiVersion, short min, short max) {
        var apiVersionResponse = new ApiVersionsResponseData();
        var apiVersions = ApiKeys.API_VERSIONS;
        var version = new ApiVersionsResponseData.ApiVersion();
        version.setApiKey(apiVersions.id).setMinVersion(min).setMaxVersion(max);
        apiVersionResponse.apiKeys().add(version);
        return new ResponsePayload(ApiKeys.API_VERSIONS, apiVersion, apiVersionResponse);
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

    private static void assertKroxyliciousResponseOffersApiVersionsForApiKey(Response response, ApiKeys expectedApiKey, short expectedMinVersion,
                                                                             short expectedMaxVersion) {
        ResponsePayload payload = response.payload();
        assertEquals(ApiKeys.API_VERSIONS, payload.apiKeys());
        assertEquals((short) 3, payload.apiVersion());
        ApiVersionsResponseData message = (ApiVersionsResponseData) payload.message();
        assertEquals(1, message.apiKeys().size());
        ApiVersionsResponseData.ApiVersion singletonVersion = message.apiKeys().iterator().next();
        assertEquals(expectedApiKey.id, singletonVersion.apiKey());
        assertEquals(expectedMinVersion, singletonVersion.minVersion());
        assertEquals(expectedMaxVersion, singletonVersion.maxVersion());
    }

}
