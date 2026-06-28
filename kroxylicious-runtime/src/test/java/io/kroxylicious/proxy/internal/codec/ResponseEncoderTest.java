/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.codec;

import java.nio.ByteBuffer;

import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import io.kroxylicious.proxy.frame.DecodedResponseFrame;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class ResponseEncoderTest extends AbstractCodecTest {
    @ParameterizedTest
    @MethodSource("requestApiVersions")
    void testApiVersions(short apiVersion) throws Exception {
        ResponseHeaderData exampleHeader = exampleResponseHeader();
        ApiVersionsResponseData exampleBody = exampleApiVersionsResponse();
        short headerVersion = ApiKeys.API_VERSIONS.responseHeaderVersion(apiVersion);
        ByteBuffer expected = serializeUsingKafkaApis(headerVersion, exampleHeader, apiVersion, exampleBody);
        testEncode(expected, new DecodedResponseFrame<>(apiVersion, exampleHeader.correlationId(), exampleHeader, exampleBody),
                new KafkaResponseEncoder(null));
    }

    @Test
    void shouldFireListenerOnEncode() throws Exception {
        // Given
        KafkaMessageListener listener = mock(KafkaMessageListener.class);

        ResponseHeaderData exampleHeader = exampleResponseHeader();
        ApiVersionsResponseData exampleBody = exampleApiVersionsResponse();
        short apiVersion = ApiKeys.API_VERSIONS.latestVersion();
        short headerVersion = ApiKeys.API_VERSIONS.responseHeaderVersion(apiVersion);
        ByteBuffer expected = serializeUsingKafkaApis(headerVersion, exampleHeader, apiVersion, exampleBody);
        int expectedSizeIncludingLength = expected.remaining();
        DecodedResponseFrame<ApiVersionsResponseData> toBeEncoded = new DecodedResponseFrame<>(apiVersion, exampleHeader.correlationId(), exampleHeader, exampleBody);

        // When
        testEncode(expected, toBeEncoded, new KafkaResponseEncoder(listener));

        // Then
        verify(listener).onMessage(toBeEncoded, expectedSizeIncludingLength);
    }

}
