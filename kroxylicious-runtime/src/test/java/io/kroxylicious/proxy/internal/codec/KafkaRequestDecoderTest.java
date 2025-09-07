/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.codec;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.IntPredicate;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.protocol.types.RawTaggedField;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.RequestHeader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.DecoderException;

import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.frame.RequestFrame;
import io.kroxylicious.proxy.internal.ApiVersionsServiceImpl;
import io.kroxylicious.test.record.RecordTestUtils;

import static java.util.function.Function.identity;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class KafkaRequestDecoderTest {

    private static final IntPredicate ALL_VERSIONS = i -> true;

    @Test
    void decodeUnknownApiVersionsRespectsOverriddenLatestVersion() {
        short latestSupportedApiVersionsOverride = (short) 2;
        ApiVersionsServiceImpl apiVersionsService = new ApiVersionsServiceImpl(Map.of(ApiKeys.API_VERSIONS, latestSupportedApiVersionsOverride));
        EmbeddedChannel embeddedChannel = newEmbeddedChannel(apiVersionsService, RequestDecoderTest.DECODE_EVERYTHING);
        RequestHeaderData header = latestVersionHeaderWithAllFields(ApiKeys.API_VERSIONS, (short) (latestSupportedApiVersionsOverride + 1));
        byte[] arbitraryBodyBytes = new byte[]{ 1, 2, 3, 4 };
        ObjectSerializationCache cache = new ObjectSerializationCache();
        short latestApiVersion = ApiKeys.API_VERSIONS.latestVersion(true);
        short requestHeaderVersion = ApiKeys.API_VERSIONS.requestHeaderVersion(latestApiVersion);
        int headerSize = header.size(cache, requestHeaderVersion);
        int messageSize = headerSize + arbitraryBodyBytes.length;
        ByteBuf buffer = Unpooled.buffer();
        ByteBufAccessorImpl accessor = new ByteBufAccessorImpl(buffer);
        accessor.writeInt(messageSize);
        header.write(accessor, cache, requestHeaderVersion);
        accessor.writeByteArray(arbitraryBodyBytes);
        embeddedChannel.writeInbound(buffer);
        Object inboundMessage = embeddedChannel.readInbound();
        assertThat(inboundMessage).isInstanceOfSatisfying(DecodedRequestFrame.class, decodedRequestFrame -> {
            assertThat(decodedRequestFrame.correlationId()).isEqualTo(2);
            assertThat(decodedRequestFrame.apiKey()).isEqualTo(ApiKeys.API_VERSIONS);
            assertThat(decodedRequestFrame.apiVersion()).isEqualTo((short) 0);
            assertThat(decodedRequestFrame.decodeResponse()).isTrue();
            assertThat(decodedRequestFrame.hasResponse()).isTrue();
            assertThat(decodedRequestFrame.header()).isInstanceOfSatisfying(RequestHeaderData.class, requestHeaderData -> {
                assertThat(requestHeaderData.correlationId()).isEqualTo(2);
                assertThat(requestHeaderData.requestApiKey()).isEqualTo(ApiKeys.API_VERSIONS.id);
                assertThat(requestHeaderData.requestApiVersion()).isEqualTo((short) 0);
                assertThat(requestHeaderData.clientId()).isEmpty();
                assertThat(requestHeaderData.unknownTaggedFields()).isEmpty();
                short version = ApiKeys.API_VERSIONS.requestHeaderVersion((short) 0);
                assertUnwritable(requestHeaderData, cache, version);
            });
            assertThat(decodedRequestFrame.body()).isInstanceOfSatisfying(ApiVersionsRequestData.class,
                    apiVersionsRequestData -> assertUnwritable(apiVersionsRequestData, cache, (short) 0));
        });
    }

    @Test
    void decodeUnknownApiVersions() {
        EmbeddedChannel embeddedChannel = newEmbeddedChannel(new ApiVersionsServiceImpl(), RequestDecoderTest.DECODE_EVERYTHING);
        RequestHeaderData header = latestVersionHeaderWithAllFields(ApiKeys.API_VERSIONS, Short.MAX_VALUE);
        byte[] arbitraryBodyBytes = new byte[]{ 1, 2, 3, 4 };
        ObjectSerializationCache cache = new ObjectSerializationCache();
        short latestApiVersion = ApiKeys.API_VERSIONS.latestVersion(true);
        short requestHeaderVersion = ApiKeys.API_VERSIONS.requestHeaderVersion(latestApiVersion);
        int headerSize = header.size(cache, requestHeaderVersion);
        int messageSize = headerSize + arbitraryBodyBytes.length;
        ByteBuf buffer = Unpooled.buffer();
        ByteBufAccessorImpl accessor = new ByteBufAccessorImpl(buffer);
        accessor.writeInt(messageSize);
        header.write(accessor, cache, requestHeaderVersion);
        accessor.writeByteArray(arbitraryBodyBytes);
        embeddedChannel.writeInbound(buffer);
        Object inboundMessage = embeddedChannel.readInbound();
        assertThat(inboundMessage).isInstanceOfSatisfying(DecodedRequestFrame.class, decodedRequestFrame -> {
            assertThat(decodedRequestFrame.correlationId()).isEqualTo(2);
            assertThat(decodedRequestFrame.apiKey()).isEqualTo(ApiKeys.API_VERSIONS);
            assertThat(decodedRequestFrame.apiVersion()).isEqualTo((short) 0);
            assertThat(decodedRequestFrame.decodeResponse()).isTrue();
            assertThat(decodedRequestFrame.hasResponse()).isTrue();
            assertThat(decodedRequestFrame.header()).isInstanceOfSatisfying(RequestHeaderData.class, requestHeaderData -> {
                assertThat(requestHeaderData.correlationId()).isEqualTo(2);
                assertThat(requestHeaderData.requestApiKey()).isEqualTo(ApiKeys.API_VERSIONS.id);
                assertThat(requestHeaderData.requestApiVersion()).isEqualTo((short) 0);
                assertThat(requestHeaderData.clientId()).isEmpty();
                assertThat(requestHeaderData.unknownTaggedFields()).isEmpty();
                short version = ApiKeys.API_VERSIONS.requestHeaderVersion((short) 0);
                assertUnwritable(requestHeaderData, cache, version);
            });
            assertThat(decodedRequestFrame.body()).isInstanceOfSatisfying(ApiVersionsRequestData.class,
                    apiVersionsRequestData -> assertUnwritable(apiVersionsRequestData, cache, (short) 0));
        });
    }

    // after ApiVersions negotiation we should never encounter a request from the client for an api version unknown to the proxy
    @Test
    void throwsOnUnsupportedVersionOfNonApiVersionsRequests() {
        EmbeddedChannel embeddedChannel = newEmbeddedChannel(new ApiVersionsServiceImpl(), RequestDecoderTest.DECODE_EVERYTHING);
        short maxSupportedVersion = ApiKeys.METADATA.latestVersion(true);
        short unsupportedVersion = (short) (maxSupportedVersion + 1);
        RequestHeaderData header = latestVersionHeaderWithAllFields(ApiKeys.METADATA, unsupportedVersion);
        byte[] arbitraryBodyBytes = new byte[]{ 1, 2, 3, 4 };
        ObjectSerializationCache cache = new ObjectSerializationCache();
        short requestHeaderVersion = ApiKeys.METADATA.requestHeaderVersion(maxSupportedVersion);
        int headerSize = header.size(cache, requestHeaderVersion);
        int messageSize = headerSize + arbitraryBodyBytes.length;
        ByteBuf buffer = Unpooled.buffer();
        ByteBufAccessorImpl accessor = new ByteBufAccessorImpl(buffer);
        accessor.writeInt(messageSize);
        header.write(accessor, cache, requestHeaderVersion);
        accessor.writeByteArray(arbitraryBodyBytes);
        assertThatThrownBy(() -> embeddedChannel.writeInbound(buffer)).isInstanceOf(DecoderException.class).cause().isInstanceOf(IllegalStateException.class)
                .hasMessage("client apiVersion %d ahead of proxy maximum %d for api key: METADATA", unsupportedVersion, maxSupportedVersion);
    }

    private static Stream<Arguments> produceRequestCases() {
        return Stream.concat(produceRequestsDoNotRequireResponse((short) 0, false), produceRequestsDoNotRequireResponse((short) 1, true));
    }

    private static Stream<Arguments> produceRequestsDoNotRequireResponse(short acks, boolean hasResponse) {
        Function<Short, ProduceRequest> nonNullTransactionId = version -> produceRequest(version, acks, "transactionId");
        Function<Short, ProduceRequest> nullTransactionId = version -> produceRequest(version, acks, null);
        Function<Short, RequestHeader> withClientId = version -> new RequestHeader(ApiKeys.PRODUCE, version, "client", 2);
        Function<Short, RequestHeader> withNullClientId = version -> new RequestHeader(ApiKeys.PRODUCE, version, null, 2);
        Function<Short, RequestHeader> withTaggedField = version -> {
            RequestHeader header = new RequestHeader(ApiKeys.PRODUCE, version, null, 2);
            header.data().unknownTaggedFields().add(new RawTaggedField(1, new byte[]{ 1 }));
            return header;
        };
        Function<Short, RequestHeader> withTaggedFields = version -> {
            RequestHeader header = new RequestHeader(ApiKeys.PRODUCE, version, null, 2);
            List<RawTaggedField> fields = header.data().unknownTaggedFields();
            fields.add(new RawTaggedField(1, new byte[]{ 1 }));
            fields.add(new RawTaggedField(2, new byte[]{ 3 }));
            return header;
        };

        Stream<Arguments> targetedForDecode = produceRequestVersionFrames(RequestDecoderTest.DECODE_EVERYTHING,
                "acks " + acks + " targeted for decode - no transaction id", nullTransactionId, withClientId, ALL_VERSIONS, hasResponse);
        Stream<Arguments> targetedForDecodeWithTransactionId = produceRequestVersionFrames(RequestDecoderTest.DECODE_EVERYTHING,
                "acks " + acks + " targeted for decode - transaction id", nonNullTransactionId, withClientId, ALL_VERSIONS, hasResponse);
        Stream<Arguments> notTargetedForDecode = produceRequestVersionFrames(RequestDecoderTest.DECODE_NOTHING,
                "acks " + acks + " not targeted for decode - no transaction id", nullTransactionId, withClientId, ALL_VERSIONS, hasResponse);
        Stream<Arguments> notTargetedForDecodeWithTransactionId = produceRequestVersionFrames(RequestDecoderTest.DECODE_NOTHING,
                "acks " + acks + " not targeted for decode - transaction id", nonNullTransactionId, withClientId, ALL_VERSIONS, hasResponse);
        Stream<Arguments> notTargetedForDecodeWithNullClientId = produceRequestVersionFrames(RequestDecoderTest.DECODE_NOTHING,
                "acks " + acks + " not targeted for decode - null client id", nullTransactionId, withNullClientId, ALL_VERSIONS, hasResponse);
        Stream<Arguments> targetedForDecodeWithNullClientId = produceRequestVersionFrames(RequestDecoderTest.DECODE_EVERYTHING,
                "acks " + acks + " not targeted for decode - null client id", nullTransactionId, withNullClientId, ALL_VERSIONS, hasResponse);
        Stream<Arguments> withTaggedFieldInHeader = produceRequestVersionFrames(RequestDecoderTest.DECODE_EVERYTHING,
                "acks " + acks + " tagged field in header", nullTransactionId, withTaggedField, version -> version >= 9, hasResponse);
        Stream<Arguments> withTaggedFieldsInHeader = produceRequestVersionFrames(RequestDecoderTest.DECODE_EVERYTHING,
                "acks " + acks + " multiple tagged fields in header", nullTransactionId, withTaggedFields, version -> version >= 9, hasResponse);
        return Stream.of(targetedForDecode, notTargetedForDecode, targetedForDecodeWithTransactionId, notTargetedForDecodeWithTransactionId,
                notTargetedForDecodeWithNullClientId, targetedForDecodeWithNullClientId, withTaggedFieldInHeader, withTaggedFieldsInHeader).flatMap(identity());
    }

    private static ProduceRequest produceRequest(short version, short acks, String transactionId) {
        ProduceRequestData.TopicProduceDataCollection collection = new ProduceRequestData.TopicProduceDataCollection();
        ProduceRequestData.TopicProduceData newElement = new ProduceRequestData.TopicProduceData();
        newElement.setName("topic");
        ProduceRequestData.PartitionProduceData ppd = new ProduceRequestData.PartitionProduceData();
        ppd.setIndex(1);
        ppd.setRecords(RecordTestUtils.singleElementMemoryRecords("a", "b"));
        newElement.partitionData().add(ppd);
        collection.add(newElement);
        ProduceRequestData record = new ProduceRequestData().setAcks(acks).setTransactionalId(transactionId).setTopicData(collection);
        return new ProduceRequest(record, version);
    }

    private static Stream<Arguments> produceRequestVersionFrames(DecodePredicate decodePredicate, String decodeMessage,
                                                                 Function<Short, ProduceRequest> bodyFunction, Function<Short, RequestHeader> headerFunction,
                                                                 IntPredicate versionPredicate, boolean hasResponse) {

        IntStream produceVersions = IntStream.rangeClosed(ApiKeys.PRODUCE.oldestVersion(), ApiKeys.PRODUCE.latestVersion(true)).filter(versionPredicate);
        return produceVersions.mapToObj(
                value -> Arguments.argumentSet("producer version " + value + " : " + decodeMessage,
                        createProduceRequestFrameWithAcksAndTransactionId((short) value, bodyFunction, headerFunction),
                        decodePredicate, hasResponse));
    }

    @MethodSource("produceRequestCases")
    @ParameterizedTest
    void zeroAckProduceRequestsDoNotRequireResponse(ByteBuffer frameBuffer, DecodePredicate decodePredicate, boolean hasResponse) {
        EmbeddedChannel embeddedChannel = newEmbeddedChannel(new ApiVersionsServiceImpl(), decodePredicate);
        int remaining = frameBuffer.remaining();
        ByteBuf buffer = Unpooled.buffer(Integer.BYTES + remaining);
        buffer.writeInt(remaining);
        buffer.writeBytes(frameBuffer);
        embeddedChannel.writeInbound(buffer);
        Object inboundMessage = embeddedChannel.readInbound();
        assertThat(inboundMessage).isInstanceOfSatisfying(RequestFrame.class, decodedRequestFrame -> {
            assertThat(decodedRequestFrame.hasResponse()).isEqualTo(hasResponse);
        });
    }

    // this is an extra safety check to ensure we scrutinize new Produce versions and ensure we can pluck the
    // ack value out of the frame, skipping all variations of all preceding fields.
    @Test
    void zeroAckProduceRequestThrowsOnNextVersion() {
        EmbeddedChannel embeddedChannel = newEmbeddedChannel(new ApiVersionsServiceImpl(), RequestDecoderTest.DECODE_NOTHING);
        int headerLength = Short.BYTES + Short.BYTES + Integer.BYTES;
        ByteBuf buffer = Unpooled.buffer(Integer.BYTES + headerLength);
        buffer.writeInt(headerLength);
        buffer.writeShort(ApiKeys.PRODUCE.id);
        short apiVersion = 13;
        buffer.writeShort(apiVersion);
        buffer.writeInt(2); // correlationId
        assertThatThrownBy(() -> embeddedChannel.writeInbound(buffer)).isInstanceOf(AssertionError.class)
                .hasMessageContaining("Unsupported Produce version: " + apiVersion);
    }

    private static EmbeddedChannel newEmbeddedChannel(ApiVersionsServiceImpl apiVersionsService, DecodePredicate predicate) {
        return new EmbeddedChannel(
                new KafkaRequestDecoder(predicate, 1024, apiVersionsService, null));
    }

    private static RequestHeaderData latestVersionHeaderWithAllFields(ApiKeys requestApiKey, short requestApiVersion) {
        RequestHeaderData header = new RequestHeaderData();
        header.setRequestApiKey(requestApiKey.id);
        header.setRequestApiVersion(requestApiVersion);
        header.setCorrelationId(2);
        header.setClientId("clientId");
        header.unknownTaggedFields().add(new RawTaggedField(5, "arbitrary".getBytes(StandardCharsets.UTF_8)));
        return header;
    }

    private static void assertUnwritable(ApiMessage requestHeaderData, ObjectSerializationCache cache, short version) {
        assertThatThrownBy(() -> requestHeaderData.size(cache, version)).isInstanceOf(UnsupportedOperationException.class);
        ByteBufAccessorImpl byteBufAccessor = new ByteBufAccessorImpl(Unpooled.buffer());
        assertThatThrownBy(() -> requestHeaderData.write(byteBufAccessor, cache, version)).isInstanceOf(UnsupportedOperationException.class);
    }

    private static ByteBuffer createProduceRequestFrameWithAcksAndTransactionId(short version,
                                                                                Function<Short, ProduceRequest> produceRequestFunction,
                                                                                Function<Short, RequestHeader> headerFunction) {
        ProduceRequest produceRequest = produceRequestFunction.apply(version);
        return produceRequest.serializeWithHeader(headerFunction.apply(version));
    }

}
