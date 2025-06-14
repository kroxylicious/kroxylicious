/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.codec;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.stream.Stream;

import org.apache.kafka.common.message.ApiMessageType;
import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import io.kroxylicious.proxy.filter.ApiVersionsRequestFilter;
import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.filter.FilterAndInvoker;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.frame.OpaqueRequestFrame;
import io.kroxylicious.proxy.internal.ApiVersionsServiceImpl;

import edu.umd.cs.findbugs.annotations.NonNull;

import static io.kroxylicious.proxy.internal.codec.ByteBufs.writeByteBuf;
import static io.kroxylicious.proxy.model.VirtualClusterModel.DEFAULT_SOCKET_FRAME_MAX_SIZE_BYTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class RequestDecoderTest extends AbstractCodecTest {

    public static final DecodePredicate DECODE_NOTHING = new DecodePredicate() {
        @Override
        public boolean shouldDecodeRequest(ApiKeys apiKey, short apiVersion) {
            return false;
        }

        @Override
        public boolean shouldDecodeResponse(ApiKeys apiKey, short apiVersion) {
            return false;
        }
    };
    public static final DecodePredicate DECODE_EVERYTHING = new DecodePredicate() {
        @Override
        public boolean shouldDecodeRequest(ApiKeys apiKey, short apiVersion) {
            return true;
        }

        @Override
        public boolean shouldDecodeResponse(ApiKeys apiKey, short apiVersion) {
            return true;
        }
    };
    public static final KafkaRequestDecoder MAX_FRAME_SIZE_10_BYTES_DECODER = getKafkaRequestDecoder(DECODE_EVERYTHING, 10);

    static List<Object[]> produceRequestApiVersions() {
        List<Short> produceVersions = requestApiVersions(ApiMessageType.PRODUCE).toList();
        List<Object[]> cartesianProduct = new ArrayList<>();
        produceVersions
                .forEach(produceVersion -> Stream.of((short) 0, (short) 1, (short) -1).forEach(acks -> cartesianProduct.add(new Object[]{ produceVersion, acks })));
        return cartesianProduct;
    }

    static ProduceRequestData deserializeProduceRequestUsingKafkaApis(short apiVersion, ByteBuffer buffer) {
        return new ProduceRequestData(new ByteBufferAccessor(buffer), apiVersion);
    }

    @ParameterizedTest
    @MethodSource("requestApiVersions")
    void testApiVersionsExactlyOneFrame_decoded(short apiVersion) {
        Filter filter = (ApiVersionsRequestFilter) (version, header, request, context) -> context.requestFilterResultBuilder()
                .forward(header, request).completed();
        assertEquals(12,
                exactlyOneFrame_decoded(apiVersion,
                        ApiKeys.API_VERSIONS::requestHeaderVersion,
                        AbstractCodecTest::exampleRequestHeader,
                        AbstractCodecTest::exampleApiVersionsRequest,
                        AbstractCodecTest::deserializeRequestHeaderUsingKafkaApis,
                        AbstractCodecTest::deserializeApiVersionsRequestUsingKafkaApis,
                        new KafkaRequestDecoder(
                                DecodePredicate
                                        .forFilters(FilterAndInvoker.build(filter.getClass().getSimpleName(), filter)),
                                DEFAULT_SOCKET_FRAME_MAX_SIZE_BYTES, new ApiVersionsServiceImpl(), null),
                        DecodedRequestFrame.class,
                        (RequestHeaderData header) -> header, true),
                "Unexpected correlation id");
    }

    @Test
    void shouldThrowIfFirstIntGreaterThanMaxFrameSize() {
        // given
        ByteBuf buffer = toLength5ByteBuffer(11);

        Assertions.assertThatThrownBy(() -> {
            // when
            MAX_FRAME_SIZE_10_BYTES_DECODER.decode(null, buffer, new ArrayList<>());
        }).isInstanceOfSatisfying(FrameOversizedException.class, e -> {
            // then
            assertThat(e.getMaxFrameSizeBytes()).isEqualTo(10);
            assertThat(e.getReceivedFrameSizeBytes()).isEqualTo(11);
        });
    }

    @Test
    void shouldNotThrowIfFirstIntLessThanMaxFrameSize() {
        // given
        ByteBuf buffer = toLength5ByteBuffer(9);
        int readerIndexAtStart = buffer.readerIndex();
        ArrayList<Object> objects = new ArrayList<>();

        // when
        MAX_FRAME_SIZE_10_BYTES_DECODER.decode(null, buffer, objects);

        // then
        assertThat(objects).isEmpty();
        assertThat(buffer.readerIndex()).isEqualTo(readerIndexAtStart);
    }

    @Test
    void shouldNotThrowIfFirstIntEqualToMaxFrameSize() {
        // given
        ByteBuf buffer = toLength5ByteBuffer(10);
        int readerIndexAtStart = buffer.readerIndex();
        ArrayList<Object> objects = new ArrayList<>();

        // when
        MAX_FRAME_SIZE_10_BYTES_DECODER.decode(null, buffer, objects);

        // then
        assertThat(objects).isEmpty();
        assertThat(buffer.readerIndex()).isEqualTo(readerIndexAtStart);
    }

    // need 5 bytes in the buffer for the decoder to read the length and act on it
    private static ByteBuf toLength5ByteBuffer(int i) {
        return writeByteBuf(outputStream -> {
            outputStream.writeInt(i);
            outputStream.writeByte(1);
        });
    }

    @ParameterizedTest
    @MethodSource("requestApiVersions")
    void testApiVersionsExactlyOneFrame_opaque(short apiVersion) throws Exception {
        Filter filter = new ApiVersionsRequestFilter() {

            @Override
            public boolean shouldHandleApiVersionsRequest(short apiVersion) {
                return false;
            }

            @Override
            public CompletionStage<RequestFilterResult> onApiVersionsRequest(short apiVersion, RequestHeaderData header,
                                                                             ApiVersionsRequestData request,
                                                                             FilterContext context) {
                return context.requestFilterResultBuilder().forward(header, request).completed();
            }
        };
        assertEquals(12,
                exactlyOneFrame_encoded(apiVersion,
                        ApiKeys.API_VERSIONS::requestHeaderVersion,
                        AbstractCodecTest::exampleRequestHeader,
                        AbstractCodecTest::exampleApiVersionsRequest,
                        new KafkaRequestDecoder(DecodePredicate.forFilters(
                                FilterAndInvoker.build(filter.getClass().getSimpleName(), filter)), DEFAULT_SOCKET_FRAME_MAX_SIZE_BYTES, new ApiVersionsServiceImpl(),
                                null),
                        OpaqueRequestFrame.class, true),
                "Unexpected correlation id");
    }

    @ParameterizedTest
    @MethodSource("requestApiVersions")
    void testApiVersionsFrameLessOneByte(short apiVersion) {
        RequestHeaderData encodedHeader = exampleRequestHeader(apiVersion);
        ApiVersionsRequestData encodedBody = exampleApiVersionsRequest();

        short headerVersion = ApiKeys.forId(ApiKeys.API_VERSIONS.id).requestHeaderVersion(apiVersion);
        ByteBuffer bbuffer = serializeUsingKafkaApis(headerVersion, encodedHeader, apiVersion, encodedBody);

        ByteBuf byteBuf = Unpooled.wrappedBuffer(bbuffer.limit(bbuffer.limit() - 1));

        var messages = new ArrayList<>();
        Filter filter = (ApiVersionsRequestFilter) (version, header, request, context) -> context.requestFilterResultBuilder()
                .forward(header, request).completed();
        new KafkaRequestDecoder(
                DecodePredicate.forFilters(
                        FilterAndInvoker.build(filter.getClass().getSimpleName(), filter)),
                DEFAULT_SOCKET_FRAME_MAX_SIZE_BYTES, new ApiVersionsServiceImpl(), null)
                .decode(null, byteBuf, messages);

        assertEquals(List.of(), messageClasses(messages));
        assertEquals(0, byteBuf.readerIndex());
    }

    private void doTestApiVersionsFrameFirstNBytes(short apiVersion, int n, int expectRead) {
        RequestHeaderData encodedHeader = exampleRequestHeader(apiVersion);
        ApiVersionsRequestData encodedBody = exampleApiVersionsRequest();

        short headerVersion = ApiKeys.forId(ApiKeys.API_VERSIONS.id).requestHeaderVersion(apiVersion);
        ByteBuffer bbuffer = serializeUsingKafkaApis(headerVersion, encodedHeader, apiVersion, encodedBody);

        ByteBuf byteBuf = Unpooled.wrappedBuffer(bbuffer.limit(n));

        var messages = new ArrayList<>();
        Filter filter = (ApiVersionsRequestFilter) (version, header, request, context) -> context.requestFilterResultBuilder().forward(header, request)
                .completed();
        getKafkaRequestDecoder(DecodePredicate.forFilters(
                FilterAndInvoker.build(filter.getClass().getSimpleName(), filter)),
                DEFAULT_SOCKET_FRAME_MAX_SIZE_BYTES)
                .decode(null, byteBuf, messages);

        assertEquals(List.of(), messageClasses(messages));
        assertEquals(expectRead, byteBuf.readerIndex());
    }

    @NonNull
    private static KafkaRequestDecoder getKafkaRequestDecoder(DecodePredicate predicate, int socketFrameMaxSizeBytes) {
        return new KafkaRequestDecoder(
                predicate,
                socketFrameMaxSizeBytes, new ApiVersionsServiceImpl(), null);
    }

    @ParameterizedTest
    @MethodSource("requestApiVersions")
    void testApiVersionsFrameFirst3Bytes(short apiVersion) {
        doTestApiVersionsFrameFirstNBytes(apiVersion, 3, 0);
    }

    @ParameterizedTest
    @MethodSource("requestApiVersions")
    void testApiVersionsFrameFirst5Bytes(short apiVersion) {
        doTestApiVersionsFrameFirstNBytes(apiVersion, 5, 0);
    }

    @ParameterizedTest
    @MethodSource("requestApiVersions")
    void testApiVersionsExactlyTwoFrames(short apiVersion) {
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
        Filter filter = (ApiVersionsRequestFilter) (version, head, request, context) -> context.requestFilterResultBuilder().forward(header, request)
                .completed();
        new KafkaRequestDecoder(
                DecodePredicate.forFilters(
                        FilterAndInvoker.build(filter.getClass().getSimpleName(), filter)),
                DEFAULT_SOCKET_FRAME_MAX_SIZE_BYTES, new ApiVersionsServiceImpl(), null)
                .decode(null, byteBuf, messages);

        assertEquals(List.of(DecodedRequestFrame.class, DecodedRequestFrame.class), messageClasses(messages));
        DecodedRequestFrame frame = (DecodedRequestFrame) messages.get(0);
        assertEquals(header, frame.header());
        assertEquals(body, frame.body());
        frame = (DecodedRequestFrame) messages.get(1);
        assertEquals(header, frame.header());
        assertEquals(body, frame.body());

        assertEquals(byteBuf.writerIndex(), byteBuf.readerIndex());
    }

    /**
     * KafkaRequestDecoder has some special case handling for Produce requests with acks==0
     */
    @ParameterizedTest
    @MethodSource("produceRequestApiVersions")
    void testAcksParsingWhenNotDecoding(short produceVersion, short acks) throws Exception {

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
        assertEquals(45,
                exactlyOneFrame_encoded(produceVersion,
                        ApiKeys.PRODUCE::requestHeaderVersion,
                        x -> header,
                        () -> body,
                        getKafkaRequestDecoder(DECODE_NOTHING, DEFAULT_SOCKET_FRAME_MAX_SIZE_BYTES),
                        OpaqueRequestFrame.class,
                        acks != 0),
                "Unexpected correlation id");
    }

    /**
     * KafkaRequestDecoder has some special case handling for Produce requests with acks==0
     */
    @ParameterizedTest
    @MethodSource("produceRequestApiVersions")
    void testAcksParsingWhenDecoding(short produceVersion, short acks) {

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
        assertEquals(45,
                exactlyOneFrame_decoded(produceVersion,
                        ApiKeys.PRODUCE::requestHeaderVersion,
                        x -> header,
                        () -> body,
                        AbstractCodecTest::deserializeRequestHeaderUsingKafkaApis,
                        RequestDecoderTest::deserializeProduceRequestUsingKafkaApis,
                        getKafkaRequestDecoder(DECODE_EVERYTHING, DEFAULT_SOCKET_FRAME_MAX_SIZE_BYTES),
                        DecodedRequestFrame.class, ((RequestHeaderData head) -> head), acks != 0),
                "Unexpected correlation id");
    }

    @Test
    void shouldFireListenerOnDecode() {
        // Given
        var mock = mock(ApiVersionsServiceImpl.class);
        var listener = mock(KafkaMessageListener.class);

        short apiVersion = ApiKeys.API_VERSIONS.latestVersion();
        short headerVersion = ApiKeys.API_VERSIONS.responseHeaderVersion(apiVersion);
        RequestHeaderData exampleHeader = exampleRequestHeader(apiVersion);
        ApiVersionsRequestData exampleBody = exampleApiVersionsRequest();
        ByteBuffer response = serializeUsingKafkaApis(headerVersion, exampleHeader, apiVersion, exampleBody);
        int expectedSizeIncludingLength = response.remaining();
        var decoder = new KafkaRequestDecoder(RequestDecoderTest.DECODE_NOTHING, Integer.MAX_VALUE, mock, listener);

        // When
        decoder.decode(null, Unpooled.wrappedBuffer(response), new ArrayList<>());

        // Then
        verify(listener).onMessage(isA(OpaqueRequestFrame.class), eq(expectedSizeIncludingLength));
    }

}
