/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.util.Optional;
import java.util.stream.Stream;

import javax.net.ssl.SSLHandshakeException;

import org.apache.kafka.common.errors.BrokerNotAvailableException;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.netty.buffer.Unpooled;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.ssl.SslHandshakeTimeoutException;

import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.frame.OpaqueResponseFrame;
import io.kroxylicious.proxy.frame.ResponseFrame;
import io.kroxylicious.test.RequestFactory;

import static io.kroxylicious.test.assertj.ResponseAssert.assertThat;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Named.named;

class KafkaProxyExceptionMapperTest {

    private static final SSLHandshakeException HANDSHAKE_EXCEPTION = new SSLHandshakeException("it went wrong");

    private KafkaProxyExceptionMapper kafkaProxyExceptionMapper;
    private OpaqueResponseFrame resp;

    @BeforeEach
    void setUp() {
        kafkaProxyExceptionMapper = new KafkaProxyExceptionMapper();
        resp = new OpaqueResponseFrame(Unpooled.EMPTY_BUFFER, 42, 0);
    }

    @Test
    void shouldCloseWithForRegisteredException() {
        // Given
        kafkaProxyExceptionMapper.registerExceptionResponse(SSLHandshakeException.class, throwable -> Optional.of(resp));

        // When
        final Optional<ResponseFrame> result = kafkaProxyExceptionMapper.mapException(HANDSHAKE_EXCEPTION);

        // Then
        assertThat(result).hasValue(resp);
    }

    @Test
    void shouldUnwrapCauseToFindForRegisteredException() {
        // Given
        kafkaProxyExceptionMapper.registerExceptionResponse(SSLHandshakeException.class, throwable -> Optional.of(resp));

        // When
        final Optional<ResponseFrame> result = kafkaProxyExceptionMapper.mapException(new DecoderException(HANDSHAKE_EXCEPTION));

        // Then
        assertThat(result).hasValue(resp);
    }

    @Test
    void shouldMapSubclassesOfRegisteredException() {
        // Given
        kafkaProxyExceptionMapper.registerExceptionResponse(SSLHandshakeException.class, throwable -> Optional.of(resp));

        // When
        final Optional<ResponseFrame> result = kafkaProxyExceptionMapper.mapException(new DecoderException(new SslHandshakeTimeoutException("It took to long, guv!")));

        // Then
        assertThat(result).hasValue(resp);
    }

    @Test
    void shouldCompleteWithCircularCauseChainDoesnotMatch() {
        // Given
        kafkaProxyExceptionMapper.registerExceptionResponse(RuntimeException.class, throwable -> Optional.of(resp));
        Exception e1 = new Exception("1");
        Exception e2 = new Exception("2", e1);
        Exception e3 = new Exception("3", e2);
        e1.initCause(e3);

        // When
        final Optional<?> result = kafkaProxyExceptionMapper.mapException(e1);

        // Then
        assertThat(result).isEmpty();
    }

    @Test
    void shouldCompleteWithCircularCauseChainMatches() {
        // Given
        kafkaProxyExceptionMapper.registerExceptionResponse(RuntimeException.class, throwable -> Optional.of(resp));
        Exception e1 = new Exception("1");
        Exception e2 = new RuntimeException("2", e1);
        Exception e3 = new Exception("3", e2);
        e1.initCause(e3);

        // When
        final Optional<ResponseFrame> result = kafkaProxyExceptionMapper.mapException(e1);

        // Then
        assertThat(result).hasValue(resp);
    }

    @ParameterizedTest
    @MethodSource({ "decodedFrameSourceLatestVersion", "decodedFrameSourceOldestVersion" })
    void shouldGenerateErrorResponseApiKey(DecodedRequestFrame<?> request) {
        // Given
        // When
        final AbstractResponse response = kafkaProxyExceptionMapper.errorResponse(request, new BrokerNotAvailableException("handshake failure", HANDSHAKE_EXCEPTION));

        // Then
        assertThat(response)
                            .hasApiKey(request.apiKey())
                            .hasErrorCount(Errors.BROKER_NOT_AVAILABLE, 1);
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
        requestHeaderData.setCorrelationId(124);
        final short apiVersion = apiMessageAndVersion.apiVersion();
        final ApiMessage apiMessage = apiMessageAndVersion.apiMessage();
        return named(ApiKeys.forId(apiMessage.apiKey()) + "-v" + apiVersion, new DecodedRequestFrame<>(apiVersion, 1, false, requestHeaderData, apiMessage));
    }

}
