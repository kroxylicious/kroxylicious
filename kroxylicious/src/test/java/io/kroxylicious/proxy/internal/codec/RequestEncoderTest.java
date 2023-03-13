/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.codec;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.common.message.ApiMessageType;
import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.frame.OpaqueRequestFrame;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RequestEncoderTest extends AbstractCodecTest {

    @ParameterizedTest
    @MethodSource("requestApiVersions()")
    public void testApiVersions(short apiVersion) throws Exception {
        RequestHeaderData exampleHeader = exampleRequestHeader(apiVersion);
        ApiVersionsRequestData exampleBody = exampleApiVersionsRequest();
        short headerVersion = ApiKeys.API_VERSIONS.requestHeaderVersion(apiVersion);
        ByteBuffer expected = serializeUsingKafkaApis(headerVersion, exampleHeader, apiVersion, exampleBody);

        CorrelationManager correlationManager = new CorrelationManager(exampleHeader.correlationId());
        var encoder = new KafkaRequestEncoder(correlationManager);
        testEncode(expected, new DecodedRequestFrame<ApiVersionsRequestData>(apiVersion, exampleHeader.correlationId(), true, exampleHeader, exampleBody), encoder);
        var corr = correlationManager.getBrokerCorrelation(exampleHeader.correlationId());
        assertEquals(ApiKeys.API_VERSIONS.id, corr.apiKey());
        assertEquals(exampleHeader.requestApiKey(), corr.apiKey());
        assertEquals(exampleHeader.requestApiVersion(), corr.apiVersion());
        assertTrue(corr.decodeResponse());
        assertEquals(exampleHeader.correlationId(), corr.downstreamCorrelationId());
    }

    // TODO test API_VERSIONS header is v0

    public static List<Object[]> produceRequestApiVersions() {
        List<Short> produceVersions = requestApiVersions(ApiMessageType.PRODUCE).collect(Collectors.toList());
        List<Object[]> cartesianProduct = new ArrayList<>();
        produceVersions.forEach(produceVersion -> Stream.of((short) 0, (short) 1, (short) -1)
                .forEach(acks -> Stream.of(true, false).forEach(decodeResponse -> cartesianProduct.add(new Object[]{ produceVersion, acks, decodeResponse }))));
        return cartesianProduct;
    }

    /**
     * KafkaRequestEncoder has some special case handling for Produce requests with acks==0
     */
    @ParameterizedTest
    @MethodSource("produceRequestApiVersions")
    public void testAcksParsing(short produceVersion, short acks, boolean decodeResponse) throws Exception {

        var header = new RequestHeaderData()
                .setRequestApiKey(ApiKeys.PRODUCE.id)
                .setRequestApiVersion(produceVersion)
                .setCorrelationId(45);
        short headerVersion = ApiKeys.PRODUCE.requestHeaderVersion(produceVersion);
        if (headerVersion >= 1) {
            header.setClientId("323423");
        }
        var body = new ProduceRequestData()
                .setAcks(acks);
        if (produceVersion >= 3) {
            body.setTransactionalId("wnedkwjn");
        }

        ByteBuffer byteBuffer = serializeUsingKafkaApis(headerVersion, header, produceVersion, body);
        int frameSize = byteBuffer.getInt();

        ByteBuf buf = Unpooled.copiedBuffer(byteBuffer);

        var frame = new OpaqueRequestFrame(buf, 12, decodeResponse, frameSize);

        ByteBuf out = Unpooled.buffer(byteBuffer.capacity() + 4);

        var correlationManager = new CorrelationManager(78);

        new KafkaRequestEncoder(correlationManager).encode(null, frame, out);

        if (acks == 0) {
            assertTrue(correlationManager.brokerRequests.isEmpty(),
                    "Expect acks == 0 to not have a correlation");
        }
        else {
            assertNotNull(correlationManager.brokerRequests.get(78),
                    "Expect acks != 0 to have a correlation");
            assertEquals(1, correlationManager.brokerRequests.size(),
                    "Expect acks != 0 to have a correlation");
        }
    }
}
