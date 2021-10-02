package io.strimzi.kproxy.codec;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
class KafkaRequestDecoderTest {
    public static Stream<Short> requestApiVersions() {
        return IntStream.range(0, ApiVersionsRequestData.SCHEMAS.length)
                .mapToObj(index -> (short) (ApiVersionsRequestData.LOWEST_SUPPORTED_VERSION + index));
    }

    private static List<? extends Class<?>> messageClasses(ArrayList<Object> messages) {
        return messages.stream().map(Object::getClass).collect(Collectors.toList());
    }

    private static ApiVersionsRequestData apiVersionsRequest() {
        return new ApiVersionsRequestData()
                .setClientSoftwareName("foo/bar")
                .setClientSoftwareVersion("1.2.0");
    }

    private static ApiVersionsResponseData apiVersionsResponse() {
        ApiVersionsResponseData.ApiVersionCollection ak = new ApiVersionsResponseData.ApiVersionCollection();
        ak.add(new ApiVersionsResponseData.ApiVersion()
                .setApiKey(ApiKeys.PRODUCE.id)
                .setMinVersion(ApiKeys.PRODUCE.messageType.lowestSupportedVersion())
                .setMaxVersion(ApiKeys.PRODUCE.messageType.highestSupportedVersion()));
        return new ApiVersionsResponseData()
                .setErrorCode(Errors.NONE.code())
                .setThrottleTimeMs(12)
                .setFinalizedFeaturesEpoch(1)
                .setApiKeys(ak)
                .setSupportedFeatures(new ApiVersionsResponseData.SupportedFeatureKeyCollection())
                .setFinalizedFeatures(new ApiVersionsResponseData.FinalizedFeatureKeyCollection());
    }

    private static RequestHeaderData requestHeader(short apiVersion) {
        return new RequestHeaderData()
                .setClientId("fooooo")
                .setCorrelationId(12)
                .setRequestApiKey(ApiKeys.API_VERSIONS.id)
                .setRequestApiVersion(apiVersion);
    }

    private static ResponseHeaderData responseHeader() {
        return new ResponseHeaderData()
                .setCorrelationId(12);
    }

    private static RequestHeaderData deserializeRequestHeader(short headerVersion, ByteBuffer buffer) {
        return new RequestHeaderData(new ByteBufferAccessor(buffer), headerVersion);
    }

    private static ResponseHeaderData deserializeResponseHeader(short headerVersion, ByteBuffer buffer) {
        return new ResponseHeaderData(new ByteBufferAccessor(buffer), headerVersion);
    }

    private static ApiVersionsRequestData deserializeApiVersionsRequest(short apiVersion, ByteBuffer buffer) {
        return new ApiVersionsRequestData(new ByteBufferAccessor(buffer), apiVersion);
    }

    private static ApiVersionsResponseData deserializeApiVersionsResponse(short apiVersion, ByteBuffer buffer) {
        return new ApiVersionsResponseData(new ByteBufferAccessor(buffer), apiVersion);
    }

    private static ByteBuffer serialize(short headerVersion, ApiMessage header, short apiVersion, ApiMessage body) {
        return serialize(1, headerVersion, header, apiVersion, body);
    }

    private static ByteBuffer serialize(int numCopies, short headerVersion, ApiMessage header, short apiVersion, ApiMessage body) {
        var cache = new ObjectSerializationCache();
        int headerSize = header.size(cache, headerVersion);
        int bodySize = body.size(cache, apiVersion);
        ByteBuffer bbuffer = ByteBuffer.allocate(numCopies * (4 + headerSize + bodySize));
        for (int i = 0; i < numCopies; i++) {
            bbuffer.putInt(headerSize + bodySize);
            var kafkaAccessor = new ByteBufferAccessor(bbuffer);
            header.write(kafkaAccessor, cache, headerVersion);
            body.write(kafkaAccessor, cache, apiVersion);
        }
        bbuffer.flip();
        return bbuffer;
    }

    @ParameterizedTest
    @MethodSource("requestApiVersions")
    public void testApiVersionsRequestFrameExact(short apiVersion) throws Exception {
        RequestHeaderData encodedHeader = requestHeader(apiVersion);

        ApiVersionsRequestData encodedBody = apiVersionsRequest();

        short headerVersion = ApiKeys.forId(ApiKeys.API_VERSIONS.id).requestHeaderVersion(apiVersion);
        ByteBuffer bbuffer = serialize(headerVersion, encodedHeader, apiVersion, encodedBody);

        // This is a bit of a hack... the Data classes know about which fields appear in which versions
        // So use Kafka to deserialize the messages we just serialised using Kafka, so that we
        // have objects we can use assertEquals on later
        bbuffer.mark();
        bbuffer.getInt(); // frame size
        var header = deserializeRequestHeader(headerVersion, bbuffer);
        var body = deserializeApiVersionsRequest(apiVersion, bbuffer);
        bbuffer.reset();

        ByteBuf byteBuf = Unpooled.wrappedBuffer(bbuffer);

        var messages = new ArrayList<>();
        new KafkaRequestDecoder().decode(null, byteBuf, messages);

        assertEquals(List.of(KafkaFrame.class), messageClasses(messages));
        KafkaFrame frame = (KafkaFrame) messages.get(0);
        assertEquals(header, frame.header());
        assertEquals(body, frame.body());

        assertEquals(byteBuf.writerIndex(), byteBuf.readerIndex());
    }

    @ParameterizedTest
    @MethodSource("requestApiVersions")
    public void testApiVersionsResponseFrameExact(short apiVersion) throws Exception {
        ResponseHeaderData encodedHeader = responseHeader();

        var encodedBody = apiVersionsResponse();

        short headerVersion = ApiKeys.forId(ApiKeys.API_VERSIONS.id).requestHeaderVersion(apiVersion);
        ByteBuffer bbuffer = serialize(headerVersion, encodedHeader, apiVersion, encodedBody);

        // This is a bit of a hack... the Data classes know about which fields appear in which versions
        // So use Kafka to deserialize the messages we just serialised using Kafka, so that we
        // have objects we can use assertEquals on later
        bbuffer.mark();
        bbuffer.getInt(); // frame size
        var header = deserializeResponseHeader(headerVersion, bbuffer);
        var body = deserializeApiVersionsResponse(apiVersion, bbuffer);
        bbuffer.reset();

        ByteBuf byteBuf = Unpooled.wrappedBuffer(bbuffer);

        var messages = new ArrayList<>();
        new KafkaResponseDecoder(new HashMap<>(Map.of(12, new KafkaRequestEncoder.VersionedApi(ApiKeys.API_VERSIONS.id, apiVersion)))).decode(null, byteBuf, messages);

        assertEquals(List.of(KafkaFrame.class), messageClasses(messages));
        KafkaFrame frame = (KafkaFrame) messages.get(0);
        assertEquals(header, frame.header());
        assertEquals(body, frame.body());

        assertEquals(byteBuf.writerIndex(), byteBuf.readerIndex());
    }

    @ParameterizedTest
    @MethodSource("requestApiVersions")
    public void testApiVersionsRequestFrameLessOneByte(short apiVersion) throws Exception {
        RequestHeaderData encodedHeader = requestHeader(apiVersion);
        ApiVersionsRequestData encodedBody = apiVersionsRequest();

        short headerVersion = ApiKeys.forId(ApiKeys.API_VERSIONS.id).requestHeaderVersion(apiVersion);
        ByteBuffer bbuffer = serialize(headerVersion, encodedHeader, apiVersion, encodedBody);

        ByteBuf byteBuf = Unpooled.wrappedBuffer(bbuffer.limit(bbuffer.limit() - 1));

        var messages = new ArrayList<>();
        new KafkaRequestDecoder().decode(null, byteBuf, messages);

        assertEquals(List.of(), messageClasses(messages));
        assertEquals(4, byteBuf.readerIndex());
    }

    private void testApiVersionsRequestFrameFirstNBytes(short apiVersion, int n, int expectRead) throws Exception {
        RequestHeaderData encodedHeader = requestHeader(apiVersion);
        ApiVersionsRequestData encodedBody = apiVersionsRequest();

        short headerVersion = ApiKeys.forId(ApiKeys.API_VERSIONS.id).requestHeaderVersion(apiVersion);
        ByteBuffer bbuffer = serialize(headerVersion, encodedHeader, apiVersion, encodedBody);

        ByteBuf byteBuf = Unpooled.wrappedBuffer(bbuffer.limit(n));

        var messages = new ArrayList<>();
        new KafkaRequestDecoder().decode(null, byteBuf, messages);

        assertEquals(List.of(), messageClasses(messages));
        assertEquals(expectRead, byteBuf.readerIndex());
    }

    @ParameterizedTest
    @MethodSource("requestApiVersions")
    public void testApiVersionsRequestFrameFirst3Bytes(short apiVersion) throws Exception {
        testApiVersionsRequestFrameFirstNBytes(apiVersion, 3, 0);
    }

    @ParameterizedTest
    @MethodSource("requestApiVersions")
    public void testApiVersionsRequestFrameFirst5Bytes(short apiVersion) throws Exception {
        testApiVersionsRequestFrameFirstNBytes(apiVersion, 5, 4);
    }

    @ParameterizedTest
    @MethodSource("requestApiVersions")
    public void testTwoApiVersionsRequestFrameExact(short apiVersion) throws Exception {
        RequestHeaderData encodedHeader = requestHeader(apiVersion);

        ApiVersionsRequestData encodedBody = apiVersionsRequest();

        short headerVersion = ApiKeys.forId(ApiKeys.API_VERSIONS.id).requestHeaderVersion(apiVersion);
        ByteBuffer bbuffer = serialize(2, headerVersion, encodedHeader, apiVersion, encodedBody);

        // This is a bit of a hack... the Data classes know about which fields appear in which versions
        // So use Kafka to deserialize the messages we just serialised using Kafka, so that we
        // have objects we can use assertEquals on later
        bbuffer.mark();
        bbuffer.getInt(); // frame size
        var header = deserializeRequestHeader(headerVersion, bbuffer);
        var body = deserializeApiVersionsRequest(apiVersion, bbuffer);
        bbuffer.reset();

        ByteBuf byteBuf = Unpooled.wrappedBuffer(bbuffer);

        var messages = new ArrayList<>();
        new KafkaRequestDecoder().decode(null, byteBuf, messages);

        assertEquals(List.of(KafkaFrame.class, KafkaFrame.class), messageClasses(messages));
        KafkaFrame frame = (KafkaFrame) messages.get(0);
        assertEquals(header, frame.header());
        assertEquals(body, frame.body());
        frame = (KafkaFrame) messages.get(1);
        assertEquals(header, frame.header());
        assertEquals(body, frame.body());

        assertEquals(byteBuf.writerIndex(), byteBuf.readerIndex());
    }
}