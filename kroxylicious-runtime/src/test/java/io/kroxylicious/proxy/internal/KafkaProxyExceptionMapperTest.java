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
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.test.RequestFactory;

import static io.kroxylicious.test.assertj.ResponseAssert.assertThat;
import static org.junit.jupiter.api.Named.named;

class KafkaProxyExceptionMapperTest {

    private static final SSLHandshakeException HANDSHAKE_EXCEPTION = new SSLHandshakeException("it went wrong");

    @ParameterizedTest
    @MethodSource({ "decodedFrameSourceLatestVersion", "decodedFrameSourceOldestVersion" })
    void shouldGenerateErrorResponseApiKey(DecodedRequestFrame<?> request) {
        // Given
        // When
        final AbstractResponse response = KafkaProxyExceptionMapper.errorResponse(request, new BrokerNotAvailableException("handshake failure", HANDSHAKE_EXCEPTION));

        // Then
        assertThat(response)
                .hasApiKey(request.apiKey())
                .hasErrorCount(Errors.BROKER_NOT_AVAILABLE, 1);
    }

    @ParameterizedTest
    @MethodSource({ "decodedFrameSourceLatestVersion", "decodedFrameSourceOldestVersion" })
    void shouldGenerateErrorMessage(DecodedRequestFrame<?> request) {
        // Given
        // When
        final AbstractResponse response = KafkaProxyExceptionMapper.errorResponseForMessage(request.header(), request.body(), new UnknownServerException("Bailing out!"));

        // Then
        assertThat(response)
                .hasApiKey(request.apiKey())
                .hasErrorCount(Errors.UNKNOWN_SERVER_ERROR, 1);
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
