/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.strimzi.kproxy.codec;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class KafkaRequestEncoderTest extends CodecTest {


    private void testEncode(short apiVersion, ApiMessage encodedHeader, ApiMessage encodedBody, KafkaMessageEncoder encoder) throws Exception {
        ApiKeys apiKey = ApiKeys.forId(encodedBody.apiKey());

        // Encode using the Kafka APIs
        short headerVersion = apiKey.requestHeaderVersion(apiVersion);
        ByteBuffer kafkaBuffer = serializeUsingKafkaApis(headerVersion, encodedHeader, apiVersion, encodedBody);
        assertEquals(kafkaBuffer.limit(), kafkaBuffer.capacity());

        // Encode using our APIS
        var frame = new KafkaFrame(apiVersion, encodedHeader, encodedBody);
        ByteBuffer ourBuffer = ByteBuffer.allocate(kafkaBuffer.capacity()).clear();

        ByteBuf out = Unpooled.wrappedBuffer(ourBuffer).resetWriterIndex();
        encoder.encode(null, frame, out);

        // Assert that we filled the buffer (since it was supposed to be the exact size)
        assertEquals(ourBuffer.limit(), ourBuffer.capacity());

        // Compare the buffers byte-for-byte
        assertArrayEquals(
                kafkaBuffer.array(), ourBuffer.array(),
                String.format("Expected: %s%nBut was: %s", Arrays.toString(kafkaBuffer.array()), Arrays.toString(ourBuffer.array())));
    }

    @ParameterizedTest
    @MethodSource("requestApiVersions")
    public void testEncodeApiVersionsRequest(short apiVersion) throws Exception {
        RequestHeaderData encodedHeader = exampleRequestHeader(apiVersion);
        ApiVersionsRequestData encodedBody = exampleApiVersionsRequest();
        HashMap<Integer, KafkaRequestEncoder.VersionedApi> correlation = new HashMap<>();
        var encoder = new KafkaRequestEncoder(correlation);
        testEncode(apiVersion, encodedHeader, encodedBody, encoder);
        assertEquals(Map.of(encodedHeader.correlationId(), new KafkaRequestEncoder.VersionedApi(encodedBody.apiKey(), apiVersion)), correlation);
    }

    @ParameterizedTest
    @MethodSource("requestApiVersions")
    public void testEncodeApiVersionsResponse(short apiVersion) throws Exception {
        ResponseHeaderData encodedHeader = exampleResponseHeader();
        ApiVersionsResponseData encodedBody = exampleApiVersionsResponse();
        testEncode(apiVersion, encodedHeader, encodedBody, new KafkaResponseEncoder());
    }

    // TODO test API_VERSIONS header is v0

    // TODO test PRODUCE with acks=0 doesn't end up in correlation
    // TODO test PRODUCE with acks!=0 doe end up in correlation
}
