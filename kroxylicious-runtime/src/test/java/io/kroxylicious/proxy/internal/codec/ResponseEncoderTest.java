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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import io.kroxylicious.proxy.frame.DecodedResponseFrame;
import io.kroxylicious.proxy.frame.RequestResponseState;

class ResponseEncoderTest extends AbstractCodecTest {
    @ParameterizedTest
    @MethodSource("requestApiVersions")
    void testApiVersions(short apiVersion) throws Exception {
        ResponseHeaderData exampleHeader = exampleResponseHeader();
        ApiVersionsResponseData exampleBody = exampleApiVersionsResponse();
        short headerVersion = ApiKeys.API_VERSIONS.responseHeaderVersion(apiVersion);
        ByteBuffer expected = serializeUsingKafkaApis(headerVersion, exampleHeader, apiVersion, exampleBody);
        testEncode(expected, new DecodedResponseFrame<>(apiVersion, exampleHeader.correlationId(), exampleHeader, exampleBody, RequestResponseState.empty()),
                new KafkaResponseEncoder());
    }
}
