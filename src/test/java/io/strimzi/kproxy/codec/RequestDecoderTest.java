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
import java.util.HashMap;
import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RequestDecoderTest extends AbstractCodecTest {

    @ParameterizedTest
    @MethodSource("requestApiVersions")
    public void testApiVersionsExactlyOneFrame_decoded(short apiVersion) throws Exception {
        exactlyOneFrame_decoded(apiVersion,
                ApiKeys.API_VERSIONS::requestHeaderVersion,
                AbstractCodecTest::exampleRequestHeader,
                AbstractCodecTest::exampleApiVersionsRequest,
                AbstractCodecTest::deserializeRequestHeaderUsingKafkaApis,
                AbstractCodecTest::deserializeApiVersionsRequestUsingKafkaApis,
                new KafkaRequestDecoder(AbstractCodecTest.ALWAYS_DECODE, new HashMap<>()),
                DecodedRequestFrame.class);
    }

    @ParameterizedTest
    @MethodSource("requestApiVersions")
    public void testApiVersionsExactlyOneFrame_opaque(short apiVersion) throws Exception {
        exactlyOneFrame_encoded(apiVersion,
                ApiKeys.API_VERSIONS::requestHeaderVersion,
                AbstractCodecTest::exampleRequestHeader,
                AbstractCodecTest::exampleApiVersionsRequest,
                new KafkaRequestDecoder(AbstractCodecTest.NEVER_DECODE, new HashMap<>()),
                OpaqueRequestFrame.class);
    }


    @ParameterizedTest
    @MethodSource("requestApiVersions")
    public void testApiVersionsFrameLessOneByte(short apiVersion) throws Exception {
        RequestHeaderData encodedHeader = exampleRequestHeader(apiVersion);
        ApiVersionsRequestData encodedBody = exampleApiVersionsRequest();

        short headerVersion = ApiKeys.forId(ApiKeys.API_VERSIONS.id).requestHeaderVersion(apiVersion);
        ByteBuffer bbuffer = serializeUsingKafkaApis(headerVersion, encodedHeader, apiVersion, encodedBody);

        ByteBuf byteBuf = Unpooled.wrappedBuffer(bbuffer.limit(bbuffer.limit() - 1));

        var messages = new ArrayList<>();
        new KafkaRequestDecoder(AbstractCodecTest.ALWAYS_DECODE, new HashMap<>()).decode(null, byteBuf, messages);

        assertEquals(List.of(), messageClasses(messages));
        assertEquals(4, byteBuf.readerIndex());
    }

    private void doTestApiVersionsFrameFirstNBytes(short apiVersion, int n, int expectRead) throws Exception {
        RequestHeaderData encodedHeader = exampleRequestHeader(apiVersion);
        ApiVersionsRequestData encodedBody = exampleApiVersionsRequest();

        short headerVersion = ApiKeys.forId(ApiKeys.API_VERSIONS.id).requestHeaderVersion(apiVersion);
        ByteBuffer bbuffer = serializeUsingKafkaApis(headerVersion, encodedHeader, apiVersion, encodedBody);

        ByteBuf byteBuf = Unpooled.wrappedBuffer(bbuffer.limit(n));

        var messages = new ArrayList<>();
        new KafkaRequestDecoder(AbstractCodecTest.ALWAYS_DECODE, new HashMap<>()).decode(null, byteBuf, messages);

        assertEquals(List.of(), messageClasses(messages));
        assertEquals(expectRead, byteBuf.readerIndex());
    }

    @ParameterizedTest
    @MethodSource("requestApiVersions")
    public void testApiVersionsFrameFirst3Bytes(short apiVersion) throws Exception {
        doTestApiVersionsFrameFirstNBytes(apiVersion, 3, 0);
    }

    @ParameterizedTest
    @MethodSource("requestApiVersions")
    public void testApiVersionsFrameFirst5Bytes(short apiVersion) throws Exception {
        doTestApiVersionsFrameFirstNBytes(apiVersion, 5, 4);
    }

    @ParameterizedTest
    @MethodSource("requestApiVersions")
    public void testApiVersionsExactlyTwoFrames(short apiVersion) throws Exception {
        RequestHeaderData encodedHeader = exampleRequestHeader(apiVersion);

        ApiVersionsRequestData encodedBody = exampleApiVersionsRequest();

        short headerVersion = ApiKeys.forId(ApiKeys.API_VERSIONS.id).requestHeaderVersion(apiVersion);
        ByteBuffer bbuffer = serializeUsingKafkaApis(2, headerVersion, encodedHeader, apiVersion, encodedBody);

        // This is a bit of a hack... the Data classes know about which fields appear in which versions
        // So use Kafka to deserialize the messages we just serialised using Kafka, so that we
        // have objects we can use assertEquals on later
        bbuffer.mark();
        bbuffer.getInt(); // frame size
        var header = deserializeRequestHeaderUsingKafkaApis(headerVersion, bbuffer);
        var body = deserializeApiVersionsRequestUsingKafkaApis(apiVersion, bbuffer);
        bbuffer.reset();

        ByteBuf byteBuf = Unpooled.wrappedBuffer(bbuffer);

        var messages = new ArrayList<>();
        new KafkaRequestDecoder(AbstractCodecTest.ALWAYS_DECODE, new HashMap<>()).decode(null, byteBuf, messages);

        assertEquals(List.of(DecodedRequestFrame.class, DecodedRequestFrame.class), messageClasses(messages));
        DecodedRequestFrame frame = (DecodedRequestFrame) messages.get(0);
        assertEquals(header, frame.header());
        assertEquals(body, frame.body());
        frame = (DecodedRequestFrame) messages.get(1);
        assertEquals(header, frame.header());
        assertEquals(body, frame.body());

        assertEquals(byteBuf.writerIndex(), byteBuf.readerIndex());
    }
}