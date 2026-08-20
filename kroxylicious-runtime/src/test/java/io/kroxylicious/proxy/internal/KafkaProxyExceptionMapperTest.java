/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.util.stream.Stream;

import javax.net.ssl.SSLHandshakeException;

import org.junit.jupiter.api.Named;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.kroxylicious.kafka.common.errors.BrokerNotAvailableException;
import io.kroxylicious.kafka.common.errors.UnknownServerException;
import io.kroxylicious.kafka.common.message.RequestHeaderData;
import io.kroxylicious.kafka.common.protocol.ApiKeys;
import io.kroxylicious.kafka.common.protocol.ApiMessage;
import io.kroxylicious.kafka.common.protocol.Errors;
import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.testing.filter.RequestFactory;

import static io.kroxylicious.testing.filter.assertj.ResponseAssert.assertThat;
import static org.junit.jupiter.api.Named.named;

class KafkaProxyExceptionMapperTest {

    @ParameterizedTest
    @MethodSource({ "decodedFrameSourceLatestVersion", "decodedFrameSourceOldestVersion" })
    void shouldGenerateErrorResponseApiKey(DecodedRequestFrame<?> request) {
        // Given
        // When
        final ApiMessage response = KafkaProxyExceptionMapper.errorResponse(request,
                new BrokerNotAvailableException("handshake failure", new SSLHandshakeException("it went wrong")));

        // Then
        assertThat(response, request.apiVersion())
                .hasApiKey(request.apiKey())
                .hasErrorCount(Errors.BROKER_NOT_AVAILABLE, 1);
    }

    @ParameterizedTest
    @MethodSource({ "decodedFrameSourceLatestVersion", "decodedFrameSourceOldestVersion" })
    void shouldGenerateErrorMessage(DecodedRequestFrame<?> request) {
        // Given
        // When
        final ApiMessage response = KafkaProxyExceptionMapper.errorResponseForMessage(request.header(), request.body(), new UnknownServerException("Bailing out!"));

        // Then
        assertThat(response, request.apiVersion())
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
        // DESCRIBE_LOG_DIRS v1 has no field capable of carrying a generic error: the top-level
        // ErrorCode field was only added at v3, and the response's per-directory Results are
        // populated from the broker's own state, not from the request, so getErrorResponse()
        // can't attach the error anywhere at this version.
        var apiKeys = RequestFactory.supportedApiKeys().stream()
                .filter(apiKey -> !(apiKey == ApiKeys.DESCRIBE_LOG_DIRS && apiKey.oldestVersion() == 1))
                .toArray(ApiKeys[]::new);
        return RequestFactory
                .apiMessageFor(ApiKeys::oldestVersion, apiKeys)
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
