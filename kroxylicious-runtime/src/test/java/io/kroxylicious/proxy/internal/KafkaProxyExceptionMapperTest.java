/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.util.stream.Stream;

import io.kroxylicious.kafka.common.message.ProduceRequestData;
import io.kroxylicious.kafka.common.message.RequestHeaderData;
import io.kroxylicious.kafka.common.protocol.ApiKeys;
import io.kroxylicious.kafka.common.protocol.ApiMessage;
import io.kroxylicious.kafka.common.protocol.Errors;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.testing.filter.RequestFactory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Named.named;

/**
 * Exercises {@link KafkaProxyExceptionMapper}'s mapping across
 * every {@link ApiKeys} that {@link RequestFactory} can populate, both at each key's oldest and
 * latest supported version. Per-RPC error-code and error-message correctness (including message
 * suppression rules) is covered exhaustively against the kafka-clients oracle by
 * {@link KafkaProxyExceptionMapperParityTest}; this test's job is narrower - confirming the mapper routes to
 * the right {@link ApiKeys} case and returns a well-formed body, using an independently-generated
 * fixture set rather than {@link KafkaProxyExceptionMapperParityTest}'s own fixtures.
 */
class KafkaProxyExceptionMapperTest {

    @ParameterizedTest
    @MethodSource({ "decodedFrameSourceLatestVersion", "decodedFrameSourceOldestVersion" })
    void shouldGenerateErrorMessage(DecodedRequestFrame<?> request) {
        // Given
        // When
        RequestHeaderData requestHeaders = request.header();
        ApiMessage message = request.body();
        ApiKeys apiKey = ApiKeys.forId(message.apiKey());
        final ApiMessage response = KafkaProxyExceptionMapper.errorResponseData(apiKey, message, requestHeaders.requestApiVersion(), Errors.UNKNOWN_SERVER_ERROR, null);

        // Then
        assertThat(response).isNotNull();
        assertThat(ApiKeys.forId(response.apiKey())).isEqualTo(request.apiKey());
    }

    @ParameterizedTest
    @MethodSource({ "decodedFrameSourceLatestVersion", "decodedFrameSourceOldestVersion" })
    void shouldGenerateErrorMessageWithMessage(DecodedRequestFrame<?> request) {
        // Given
        // When
        RequestHeaderData requestHeaders = request.header();
        ApiMessage message = request.body();
        ApiKeys apiKey = ApiKeys.forId(message.apiKey());
        final ApiMessage response = KafkaProxyExceptionMapper.errorResponseData(apiKey, message, requestHeaders.requestApiVersion(), Errors.UNKNOWN_SERVER_ERROR,
                "message");

        // Then
        assertThat(response).isNotNull();
        assertThat(ApiKeys.forId(response.apiKey())).isEqualTo(request.apiKey());
    }

    // NONE is the standard sentinel in the Kafka Protocol for no-error, which is counter to what the proxy error response handling is trying to achieve.
    @Test
    void noneErrorDisallowed() {
        // Given
        ProduceRequestData arbitraryRequest = new ProduceRequestData().setAcks((short) 0).setTimeoutMs(1000);
        short arbitraryVersion = ApiKeys.PRODUCE.latestVersion();

        // When/Then
        assertThatThrownBy(() -> KafkaProxyExceptionMapper.errorResponseData(ApiKeys.PRODUCE, arbitraryRequest, arbitraryVersion, Errors.NONE, "message"))
                .isInstanceOf(IllegalArgumentException.class).hasMessage("Error responses must target a specific error code. Using NONE represents a programming error");
    }

    public static Stream<Arguments> decodedFrameSourceLatestVersion() {
        return RequestFactory
                .apiMessageFor(ApiKeys::latestVersion)
                .map(KafkaProxyExceptionMapperTest::toDecodedFrame)
                .map(Arguments::of);
    }

    public static Stream<Arguments> decodedFrameSourceOldestVersion() {
        return RequestFactory
                .apiMessageFor(ApiKeys::oldestVersion)
                .map(KafkaProxyExceptionMapperTest::toDecodedFrame)
                .map(Arguments::of);
    }

    private static Named<DecodedRequestFrame<ApiMessage>> toDecodedFrame(RequestFactory.ApiMessageVersion apiMessageAndVersion) {
        final RequestHeaderData requestHeaderData = new RequestHeaderData();
        final short apiVersion = apiMessageAndVersion.apiVersion();
        requestHeaderData.setRequestApiVersion(apiVersion);
        requestHeaderData.setCorrelationId(124);
        final ApiMessage apiMessage = apiMessageAndVersion.apiMessage();
        return named(ApiKeys.forId(apiMessage.apiKey()) + "-v" + apiVersion, new DecodedRequestFrame<>(apiVersion, 1, false, requestHeaderData, apiMessage));
    }

}
