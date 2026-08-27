/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.util.stream.Stream;

import javax.net.ssl.SSLHandshakeException;

import org.apache.kafka.common.errors.BrokerNotAvailableException;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.testing.filter.RequestFactory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Named.named;

/**
 * Exercises {@link KafkaProxyExceptionMapper}'s dispatch to {@link ErrorResponseFactory} across
 * every {@link ApiKeys} that {@link RequestFactory} can populate, both at each key's oldest and
 * latest supported version. Per-RPC error-code and error-message correctness (including message
 * suppression rules) is covered exhaustively against the kafka-clients oracle by
 * {@link ErrorResponseFactoryTest}; this test's job is narrower - confirming the mapper routes to
 * the right {@link ApiKeys} case and returns a well-formed body, using an independently-generated
 * fixture set rather than {@link ErrorResponseFactoryTest}'s own fixtures.
 */
class KafkaProxyExceptionMapperTest {

    @ParameterizedTest
    @MethodSource({ "decodedFrameSourceLatestVersion", "decodedFrameSourceOldestVersion" })
    void shouldGenerateErrorResponseApiKey(DecodedRequestFrame<?> request) {
        // Given
        // When
        final ApiMessage response = KafkaProxyExceptionMapper.errorResponseMessage(request,
                new BrokerNotAvailableException("handshake failure", new SSLHandshakeException("it went wrong")));

        // Then
        assertThat(response).isNotNull();
        assertThat(ApiKeys.forId(response.apiKey())).isEqualTo(request.apiKey());
    }

    @ParameterizedTest
    @MethodSource({ "decodedFrameSourceLatestVersion", "decodedFrameSourceOldestVersion" })
    void shouldGenerateErrorMessage(DecodedRequestFrame<?> request) {
        // Given
        // When
        final ApiMessage response = KafkaProxyExceptionMapper.errorResponseForMessage(request.header(), request.body(), new UnknownServerException("Bailing out!"));

        // Then
        assertThat(response).isNotNull();
        assertThat(ApiKeys.forId(response.apiKey())).isEqualTo(request.apiKey());
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
