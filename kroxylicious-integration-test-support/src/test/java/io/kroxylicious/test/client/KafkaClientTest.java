/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.test.client;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.RequestHeader;
import org.junit.jupiter.api.Test;

import io.netty.buffer.Unpooled;

import io.kroxylicious.test.Request;
import io.kroxylicious.test.Response;
import io.kroxylicious.test.ResponsePayload;
import io.kroxylicious.test.codec.OpaqueRequestFrame;
import io.kroxylicious.test.server.MockServer;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaClientTest {
    @Test
    void testClientCanSendOpaqueFrame() {
        ApiVersionsResponseData message = new ApiVersionsResponseData();
        message.setErrorCode(Errors.UNSUPPORTED_VERSION.code());
        try (var mockServer = MockServer.startOnRandomPort(new ResponsePayload(ApiKeys.API_VERSIONS, (short) 0, message));
                var kafkaClient = new KafkaClient("127.0.0.1", mockServer.port())) {
            ApiVersionsRequest request = new ApiVersionsRequest(new ApiVersionsRequestData(), (short) 0);
            int correlationId = 5;
            ByteBuffer byteBuffer = request.serializeWithHeader(new RequestHeader(ApiKeys.API_VERSIONS, (short) 0, "client", correlationId));
            int length = byteBuffer.remaining();
            OpaqueRequestFrame frame = new OpaqueRequestFrame(Unpooled.copiedBuffer(byteBuffer), correlationId, length, true, ApiKeys.API_VERSIONS, (short) 0);
            CompletableFuture<Response> future = kafkaClient.get(frame);
            assertThat(future).succeedsWithin(10, TimeUnit.SECONDS).satisfies(response -> {
                assertThat(response.payload().message()).isInstanceOfSatisfying(ApiVersionsResponseData.class, apiVersionsRequestData -> {
                    assertThat(apiVersionsRequestData.errorCode()).isEqualTo(Errors.UNSUPPORTED_VERSION.code());
                });
            });
        }
    }

    // brokers can respond with a v0 response if they do not support the ApiVersions request version, see KIP-511
    @Test
    void testClientCanTolerateV0ApiVersionsResponseToHigherRequestVersion() {
        ApiVersionsResponseData message = new ApiVersionsResponseData();
        message.setErrorCode(Errors.UNSUPPORTED_VERSION.code());
        ResponsePayload v0Payload = new ResponsePayload(ApiKeys.API_VERSIONS, (short) 0, message);
        try (var mockServer = MockServer.startOnRandomPort(v0Payload);
                var kafkaClient = new KafkaClient("127.0.0.1", mockServer.port())) {
            CompletableFuture<Response> future = kafkaClient.get(new Request(ApiKeys.API_VERSIONS, (short) 0, "client", new ApiVersionsRequestData()));
            assertThat(future).succeedsWithin(10, TimeUnit.SECONDS).satisfies(response -> {
                assertThat(response.payload().message()).isInstanceOfSatisfying(ApiVersionsResponseData.class, apiVersionsRequestData -> {
                    assertThat(apiVersionsRequestData.errorCode()).isEqualTo(Errors.UNSUPPORTED_VERSION.code());
                });
            });
        }
    }
}
