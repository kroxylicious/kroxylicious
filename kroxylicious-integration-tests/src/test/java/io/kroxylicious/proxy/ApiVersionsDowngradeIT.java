/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.junit.jupiter.api.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import io.kroxylicious.test.Request;
import io.kroxylicious.test.Response;
import io.kroxylicious.test.ResponsePayload;
import io.kroxylicious.test.codec.ByteBufAccessorImpl;
import io.kroxylicious.test.codec.OpaqueRequestFrame;
import io.kroxylicious.test.tester.KroxyliciousConfigUtils;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.common.SaslMechanism;

import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.proxy;
import static io.kroxylicious.test.tester.KroxyliciousTesters.kroxyliciousTester;
import static io.kroxylicious.test.tester.KroxyliciousTesters.mockKafkaKroxyliciousTester;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * <p>
 * In addition to the behaviour tested by {@link ApiVersionsIT} we also need to handle the client
 * sending ApiVersions requests at an apiVersion higher than the proxy understands. With no proxy
 * involved, the behaviour is the broker will respond with a v0 apiversions response and the error
 * code is set to UNSUPPORTED_VERSION, so that the client can retry.
 * </p>
 * <br>
 * In this case the proxy will:
 * <ol>
 *     <li>detect that it does not know the apiVersion in the frame</li>
 *     <li>create and forward an ApiVersions request at the highest api version the proxy is aware of</li>
 *     <li>correlate the response and ensure we respond with UNSUPPORTED_VERSION error and a v0 response.</li>
 * </ol>
 * <p>
 * Step 3 has two flavours, if the broker responds with UNSUPPORTED_VERSION we assume the response is
 * v0 and pass it through. If the broker responds successfully to our request, we downgrade it to a
 * v0 response and set the error code in the proxy.
 * </p>
 */
public class ApiVersionsDowngradeIT extends BaseIT {

    @Test
    void withRealBrokerAtSameVersionAsProxyClientAhead(KafkaCluster cluster) {
        try (var tester = kroxyliciousTester(proxy(cluster));
                var client = tester.simpleTestClient()) {
            ApiVersionsResponseData mockResponse = new ApiVersionsResponseData();
            ApiVersionsResponseData.ApiVersion version = new ApiVersionsResponseData.ApiVersion();
            version.setApiKey(ApiKeys.METADATA.id).setMinVersion(ApiKeys.METADATA.oldestVersion()).setMaxVersion((short) (ApiKeys.METADATA.latestVersion() + 1));
            mockResponse.apiKeys().add(version);
            short apiKey = ApiKeys.API_VERSIONS.id;
            int correlationId = 100;
            RequestHeaderData requestHeaderData = new RequestHeaderData();
            short unsupportedVersion = (short) (ApiKeys.API_VERSIONS.latestVersion(true) + 1);
            requestHeaderData.setRequestApiKey(apiKey);
            requestHeaderData.setClientId("THANG");
            requestHeaderData.setRequestApiVersion(unsupportedVersion);
            requestHeaderData.setCorrelationId(correlationId);
            ObjectSerializationCache cache = new ObjectSerializationCache();
            short requestHeaderVersion = ApiKeys.API_VERSIONS.requestHeaderVersion(unsupportedVersion);
            int headerSize = requestHeaderData.size(cache, requestHeaderVersion);
            int messageSize = headerSize + 4;
            ByteBuf buffer = Unpooled.buffer(messageSize, messageSize);
            int arbitraryBodyData = Integer.MAX_VALUE;
            ByteBufAccessorImpl accessor = new ByteBufAccessorImpl(buffer);
            requestHeaderData.write(accessor, cache, requestHeaderVersion);
            accessor.writeInt(arbitraryBodyData);

            CompletableFuture<Response> responseCompletableFuture = client.get(
                    new OpaqueRequestFrame(buffer, correlationId, messageSize, true, ApiKeys.API_VERSIONS, unsupportedVersion));
            assertThat(responseCompletableFuture).succeedsWithin(5, TimeUnit.SECONDS).satisfies(response1 -> {
                assertThat(response1.payload().apiKeys()).isEqualTo(ApiKeys.API_VERSIONS);
                assertThat(response1.payload().message()).isInstanceOfSatisfying(ApiVersionsResponseData.class, (data) -> {
                    assertThat(data.errorCode()).isEqualTo(Errors.UNSUPPORTED_VERSION.code());
                    ApiVersionsResponseData.ApiVersion expected = new ApiVersionsResponseData.ApiVersion();
                    expected.setApiKey(ApiKeys.API_VERSIONS.id).setMinVersion(ApiKeys.API_VERSIONS.oldestVersion()).setMaxVersion(ApiKeys.API_VERSIONS.latestVersion());
                    ApiVersionsResponseData.ApiVersionCollection collection = new ApiVersionsResponseData.ApiVersionCollection();
                    collection.add(expected);
                    assertThat(data.apiKeys()).isEqualTo(collection);
                });
            });
            // simulate follow-up request to check the broker tolerates the client retrying
            ApiVersionsRequestData message = new ApiVersionsRequestData();
            message.setClientSoftwareName("testclient");
            message.setClientSoftwareVersion("v1");
            CompletableFuture<Response> responseCompletableFuture1 = client.get(
                    new Request(ApiKeys.API_VERSIONS, ApiKeys.API_VERSIONS.latestVersion(), "client", message));
            assertThat(responseCompletableFuture1).succeedsWithin(5, TimeUnit.SECONDS).satisfies(response1 -> {
                assertThat(response1.payload().apiKeys()).isEqualTo(ApiKeys.API_VERSIONS);
                assertThat(response1.payload().message()).isInstanceOfSatisfying(ApiVersionsResponseData.class, (data) -> {
                    assertThat(data.errorCode()).isEqualTo((short) 0);
                    // calculating the exact APIs coming back from the broker is a bit tricky, it will evolve over time
                    // maybe we could hit the broker directly and figure out our expected output from that
                    assertThat(data.apiKeys()).hasSizeGreaterThan(1);
                });
            });
        }
    }

    @Test
    void withRealSaslBrokerAtSameVersionAsProxyClientAhead(@SaslMechanism(principals = {
            @SaslMechanism.Principal(user = "user", password = "pass") }) KafkaCluster cluster) {
        try (var tester = kroxyliciousTester(proxy(cluster));
                var client = tester.simpleTestClient()) {
            ApiVersionsResponseData mockResponse = new ApiVersionsResponseData();
            ApiVersionsResponseData.ApiVersion version = new ApiVersionsResponseData.ApiVersion();
            version.setApiKey(ApiKeys.METADATA.id).setMinVersion(ApiKeys.METADATA.oldestVersion()).setMaxVersion((short) (ApiKeys.METADATA.latestVersion() + 1));
            mockResponse.apiKeys().add(version);
            short apiKey = ApiKeys.API_VERSIONS.id;
            int correlationId = 100;
            RequestHeaderData requestHeaderData = new RequestHeaderData();
            short unsupportedVersion = (short) (ApiKeys.API_VERSIONS.latestVersion(true) + 1);
            requestHeaderData.setRequestApiKey(apiKey);
            requestHeaderData.setClientId("THANG");
            requestHeaderData.setRequestApiVersion(unsupportedVersion);
            requestHeaderData.setCorrelationId(correlationId);
            ObjectSerializationCache cache = new ObjectSerializationCache();
            short requestHeaderVersion = ApiKeys.API_VERSIONS.requestHeaderVersion(unsupportedVersion);
            int headerSize = requestHeaderData.size(cache, requestHeaderVersion);
            int messageSize = headerSize + 4;
            ByteBuf buffer = Unpooled.buffer(messageSize, messageSize);
            int arbitraryBodyData = Integer.MAX_VALUE;
            ByteBufAccessorImpl accessor = new ByteBufAccessorImpl(buffer);
            requestHeaderData.write(accessor, cache, requestHeaderVersion);
            accessor.writeInt(arbitraryBodyData);

            CompletableFuture<Response> responseCompletableFuture = client.get(
                    new OpaqueRequestFrame(buffer, correlationId, messageSize, true, ApiKeys.API_VERSIONS, unsupportedVersion));
            assertThat(responseCompletableFuture).succeedsWithin(5, TimeUnit.SECONDS).satisfies(response1 -> {
                assertThat(response1.payload().apiKeys()).isEqualTo(ApiKeys.API_VERSIONS);
                assertThat(response1.payload().message()).isInstanceOfSatisfying(ApiVersionsResponseData.class, (data) -> {
                    assertThat(data.errorCode()).isEqualTo(Errors.UNSUPPORTED_VERSION.code());
                    ApiVersionsResponseData.ApiVersion expected = new ApiVersionsResponseData.ApiVersion();
                    expected.setApiKey(ApiKeys.API_VERSIONS.id).setMinVersion(ApiKeys.API_VERSIONS.oldestVersion()).setMaxVersion(ApiKeys.API_VERSIONS.latestVersion());
                    ApiVersionsResponseData.ApiVersionCollection collection = new ApiVersionsResponseData.ApiVersionCollection();
                    collection.add(expected);
                    assertThat(data.apiKeys()).isEqualTo(collection);
                });
            });
            // simulate follow-up request to check the broker tolerates the client retrying
            ApiVersionsRequestData message = new ApiVersionsRequestData();
            message.setClientSoftwareName("testclient");
            message.setClientSoftwareVersion("v1");
            CompletableFuture<Response> responseCompletableFuture1 = client.get(
                    new Request(ApiKeys.API_VERSIONS, ApiKeys.API_VERSIONS.latestVersion(), "client", message));
            // currently failing because the broker SASL state machine doesn't tolerate receiving a second valid ApiVersionsRequest
            assertThat(responseCompletableFuture1).succeedsWithin(5, TimeUnit.SECONDS).satisfies(response1 -> {
                assertThat(response1.payload().apiKeys()).isEqualTo(ApiKeys.API_VERSIONS);
                assertThat(response1.payload().message()).isInstanceOfSatisfying(ApiVersionsResponseData.class, (data) -> {
                    assertThat(data.errorCode()).isEqualTo((short) 0);
                    // calculating the exact APIs coming back from the broker is a bit tricky, it will evolve over time
                    // maybe we could hit the broker directly and figure out our expected output from that
                    assertThat(data.apiKeys()).hasSizeGreaterThan(1);
                });
            });
        }
    }

    @Test
    void clientAheadOfProxy_BrokerUnderstandsLatestProxyVersion() {
        try (var tester = mockKafkaKroxyliciousTester(KroxyliciousConfigUtils::proxy);
                var client = tester.simpleTestClient()) {
            ApiVersionsResponseData mockResponse = new ApiVersionsResponseData();
            ApiVersionsResponseData.ApiVersion version = new ApiVersionsResponseData.ApiVersion();
            version.setApiKey(ApiKeys.API_VERSIONS.id).setMinVersion(ApiKeys.API_VERSIONS.oldestVersion())
                    .setMaxVersion((short) (ApiKeys.API_VERSIONS.latestVersion() + 1));
            mockResponse.apiKeys().add(version);
            tester.addMockResponseForApiKey(new ResponsePayload(ApiKeys.API_VERSIONS, (short) 0, mockResponse));

            short apiKey = ApiKeys.API_VERSIONS.id;
            int correlationId = 100;
            RequestHeaderData requestHeaderData = new RequestHeaderData();
            short unsupportedVersion = (short) (ApiKeys.API_VERSIONS.latestVersion(true) + 1);
            requestHeaderData.setRequestApiKey(apiKey);
            requestHeaderData.setRequestApiVersion(unsupportedVersion);
            requestHeaderData.setCorrelationId(correlationId);
            ObjectSerializationCache cache = new ObjectSerializationCache();
            short requestHeaderVersion = ApiKeys.API_VERSIONS.requestHeaderVersion(unsupportedVersion);
            int headerSize = requestHeaderData.size(cache, requestHeaderVersion);
            int messageSize = headerSize + 4;
            ByteBuf buffer = Unpooled.buffer(messageSize, messageSize);
            int arbitraryBodyData = Integer.MAX_VALUE;
            ByteBufAccessorImpl accessor = new ByteBufAccessorImpl(buffer);
            requestHeaderData.write(accessor, cache, requestHeaderVersion);
            accessor.writeInt(arbitraryBodyData);

            CompletableFuture<Response> responseCompletableFuture = client.get(
                    new OpaqueRequestFrame(buffer, correlationId, messageSize, true, ApiKeys.API_VERSIONS, unsupportedVersion));
            assertThat(responseCompletableFuture).succeedsWithin(5, TimeUnit.SECONDS).satisfies(response1 -> {
                assertThat(response1.payload().apiKeys()).isEqualTo(ApiKeys.API_VERSIONS);
                assertThat(response1.payload().message()).isInstanceOfSatisfying(ApiVersionsResponseData.class, (data) -> {
                    assertThat(data.errorCode()).isEqualTo(Errors.UNSUPPORTED_VERSION.code());
                    ApiVersionsResponseData.ApiVersion expected = new ApiVersionsResponseData.ApiVersion();
                    expected.setApiKey(ApiKeys.API_VERSIONS.id).setMinVersion(ApiKeys.API_VERSIONS.oldestVersion()).setMaxVersion(ApiKeys.API_VERSIONS.latestVersion());
                    ApiVersionsResponseData.ApiVersionCollection collection = new ApiVersionsResponseData.ApiVersionCollection();
                    collection.add(expected);
                    assertThat(data.apiKeys()).isEqualTo(collection);
                });
            });
        }
    }

    @Test
    void clientAheadOfProxy_BrokerDoesNotUnderstandProxyVersion() {
        try (var tester = mockKafkaKroxyliciousTester(KroxyliciousConfigUtils::proxy);
                var client = tester.simpleTestClient()) {
            ApiVersionsResponseData mockResponse = new ApiVersionsResponseData();
            mockResponse.setErrorCode(Errors.UNSUPPORTED_VERSION.code());
            ApiVersionsResponseData.ApiVersion version = new ApiVersionsResponseData.ApiVersion();
            version.setApiKey(ApiKeys.METADATA.id).setMinVersion(ApiKeys.METADATA.oldestVersion()).setMaxVersion((short) (ApiKeys.METADATA.latestVersion() + 1));
            mockResponse.apiKeys().add(version);
            tester.addMockResponseForApiKey(new ResponsePayload(ApiKeys.API_VERSIONS, ApiKeys.API_VERSIONS.latestVersion(true), mockResponse, (short) 0));
            short apiKey = ApiKeys.API_VERSIONS.id;
            int correlationId = 100;
            RequestHeaderData requestHeaderData = new RequestHeaderData();
            short unsupportedVersion = (short) (ApiKeys.API_VERSIONS.latestVersion(true) + 1);
            requestHeaderData.setRequestApiKey(apiKey);
            requestHeaderData.setRequestApiVersion(unsupportedVersion);
            requestHeaderData.setCorrelationId(correlationId);
            ObjectSerializationCache cache = new ObjectSerializationCache();
            short requestHeaderVersion = ApiKeys.API_VERSIONS.requestHeaderVersion(unsupportedVersion);
            int headerSize = requestHeaderData.size(cache, requestHeaderVersion);
            int messageSize = headerSize + 4;
            ByteBuf buffer = Unpooled.buffer(messageSize, messageSize);
            int arbitraryBodyData = Integer.MAX_VALUE;
            ByteBufAccessorImpl accessor = new ByteBufAccessorImpl(buffer);
            requestHeaderData.write(accessor, cache, requestHeaderVersion);
            accessor.writeInt(arbitraryBodyData);

            CompletableFuture<Response> responseCompletableFuture = client.get(
                    new OpaqueRequestFrame(buffer, correlationId, messageSize, true, ApiKeys.API_VERSIONS, unsupportedVersion));
            assertThat(responseCompletableFuture).succeedsWithin(5, TimeUnit.SECONDS).satisfies(response1 -> {
                assertThat(response1.payload().apiKeys()).isEqualTo(ApiKeys.API_VERSIONS);
                assertThat(response1.payload().message()).isInstanceOfSatisfying(ApiVersionsResponseData.class, (data) -> {
                    assertThat(data.errorCode()).isEqualTo(Errors.UNSUPPORTED_VERSION.code());
                    ApiVersionsResponseData.ApiVersion expected = new ApiVersionsResponseData.ApiVersion();
                    expected.setApiKey(ApiKeys.METADATA.id).setMinVersion(ApiKeys.METADATA.oldestVersion()).setMaxVersion(ApiKeys.METADATA.latestVersion());
                    ApiVersionsResponseData.ApiVersionCollection collection = new ApiVersionsResponseData.ApiVersionCollection();
                    collection.add(expected);
                    assertThat(data.apiKeys()).isEqualTo(collection);
                });
            });
        }
    }

}
