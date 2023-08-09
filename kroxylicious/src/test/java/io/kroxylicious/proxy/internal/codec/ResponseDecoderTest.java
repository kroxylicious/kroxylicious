/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.codec;

import org.apache.kafka.common.protocol.ApiKeys;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import io.kroxylicious.proxy.frame.DecodedResponseFrame;
import io.kroxylicious.proxy.frame.OpaqueResponseFrame;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ResponseDecoderTest extends AbstractCodecTest {

    @ParameterizedTest
    @MethodSource("requestApiVersions")
    void testApiVersionsExactlyOneFrame_decoded(short apiVersion) {
        var mgr = new CorrelationManager(12);
        mgr.putBrokerRequest(ApiKeys.API_VERSIONS.id, apiVersion, 52, true, null, null, true);
        assertEquals(52, exactlyOneFrame_decoded(apiVersion,
                ApiKeys.API_VERSIONS::responseHeaderVersion,
                v -> AbstractCodecTest.exampleResponseHeader(),
                AbstractCodecTest::exampleApiVersionsResponse,
                AbstractCodecTest::deserializeResponseHeaderUsingKafkaApis,
                AbstractCodecTest::deserializeApiVersionsResponseUsingKafkaApis,
                new KafkaResponseDecoder(mgr),
                DecodedResponseFrame.class,
                header -> header.setCorrelationId(12), false),
                "Unexpected correlation id");
    }

    @ParameterizedTest
    @MethodSource("requestApiVersions")
    void testApiVersionsExactlyOneFrame_opaque(short apiVersion) throws Exception {
        var mgr = new CorrelationManager(12);
        mgr.putBrokerRequest(ApiKeys.API_VERSIONS.id, apiVersion, 52, true, null, null, false);
        assertEquals(52, exactlyOneFrame_encoded(apiVersion,
                ApiKeys.API_VERSIONS::responseHeaderVersion,
                v -> AbstractCodecTest.exampleResponseHeader(),
                AbstractCodecTest::exampleApiVersionsResponse,
                new KafkaResponseDecoder(mgr),
                OpaqueResponseFrame.class, false),
                "Unexpected correlation id");
    }

}
