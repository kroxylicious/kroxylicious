/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.util.stream.Stream;

import org.junit.jupiter.api.Named;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.kroxylicious.kafka.common.message.RequestHeaderData;
import io.kroxylicious.kafka.common.protocol.ApiKeys;
import io.kroxylicious.kafka.common.protocol.ApiMessage;
import io.kroxylicious.kafka.common.protocol.Errors;
import io.kroxylicious.testing.filter.RequestFactory;
import io.kroxylicious.testing.filter.VendoredRequestFactory;

import static org.assertj.core.api.Assertions.assertThat;
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
class VendoredKafkaProxyExceptionMapperTest {

    @ParameterizedTest
    @MethodSource({ "decodedFrameSourceLatestVersion", "decodedFrameSourceOldestVersion" })
    void shouldGenerateErrorMessage(TestFrame request) {
        // Given
        // When
        final ApiMessage response = VendoredKafkaProxyExceptionMapper.errorResponseForMessage(request.header(), request.body(), Errors.UNKNOWN_SERVER_ERROR, null);

        // Then
        assertThat(response).isNotNull();
        assertThat(ApiKeys.forId(response.apiKey())).isEqualTo(request.apiKey());
    }

    public static Stream<Arguments> decodedFrameSourceLatestVersion() {
        return VendoredRequestFactory
                .apiMessageFor(ApiKeys::latestVersion)
                .map(VendoredKafkaProxyExceptionMapperTest::toDecodedFrame)
                .map(Arguments::of);
    }

    public static Stream<Arguments> decodedFrameSourceOldestVersion() {
        return VendoredRequestFactory
                .apiMessageFor(ApiKeys::oldestVersion)
                .map(VendoredKafkaProxyExceptionMapperTest::toDecodedFrame)
                .map(Arguments::of);
    }

    private record TestFrame(short apiVersion, RequestHeaderData header, ApiMessage body, ApiKeys apiKey) {

    }

    private static Named<TestFrame> toDecodedFrame(VendoredRequestFactory.ApiMessageVersion apiMessageAndVersion) {
        final RequestHeaderData requestHeaderData = new RequestHeaderData();
        final short apiVersion = apiMessageAndVersion.apiVersion();
        requestHeaderData.setRequestApiVersion(apiVersion);
        requestHeaderData.setCorrelationId(124);
        final ApiMessage apiMessage = apiMessageAndVersion.apiMessage();
        ApiKeys apiKeys = ApiKeys.forId(apiMessage.apiKey());
        return named(apiKeys + "-v" + apiVersion, new TestFrame(apiVersion, requestHeaderData, apiMessage, apiKeys));
    }

}
