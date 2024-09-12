/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.codec;

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.protocol.types.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test of ByteBufAccessor
 */
public class ByteBufAccessorTest {
    @Test
    void testRead() {
        var bbuffer = ByteBuffer.allocate(1024);

        Random rng = new Random();
        for (int i = 0; i < 1000; i++) {
            // Write using Kafka accessor
            ByteBufferAccessor nio = new ByteBufferAccessor(bbuffer);
            byte writtenByte = (byte) rng.nextInt();
            nio.writeByte(writtenByte);
            byte[] writtenArray = { 1, 2, 3 };
            nio.writeByteArray(writtenArray);
            double writtenDouble = rng.nextDouble();
            nio.writeDouble(writtenDouble);
            int writtenInt = rng.nextInt();
            nio.writeInt(writtenInt);
            long writtenLong = rng.nextLong();
            nio.writeLong(writtenLong);
            short writtenShort = (short) rng.nextInt();
            nio.writeShort(writtenShort);
            int writtenUnsignedVarint = rng.nextInt();
            nio.writeUnsignedVarint(writtenUnsignedVarint);
            int writtenVarint = rng.nextInt();
            nio.writeVarint(writtenVarint);
            long writtenVarlong = rng.nextLong();
            nio.writeVarlong(writtenVarlong);

            // Read using ByteBuf accessor
            var bbuf = Unpooled.wrappedBuffer(bbuffer.flip());
            var kp = new ByteBufAccessorImpl(bbuf);
            assertEquals(writtenByte, kp.readByte());
            var readArray = kp.readArray(writtenArray.length);
            assertArrayEquals(writtenArray, readArray);
            assertEquals(writtenDouble, kp.readDouble());
            assertEquals(writtenInt, kp.readInt());
            assertEquals(writtenLong, kp.readLong());
            assertEquals(writtenShort, kp.readShort());
            assertEquals(writtenUnsignedVarint, kp.readUnsignedVarint());
            assertEquals(writtenVarint, kp.readVarint());
            assertEquals(writtenVarlong, kp.readVarlong());

            bbuffer.clear();
        }
    }

    @Test
    void testReadTooLongVarLong() {
        var bbuffer = ByteBuffer.allocate(1024);
        ByteBufferAccessor nio = new ByteBufferAccessor(bbuffer);
        for (int j = 0; j < 10; j++) {
            nio.writeByte((byte) -5);
        }

        var bbuf = Unpooled.wrappedBuffer(bbuffer.flip());
        var kp = new ByteBufAccessorImpl(bbuf);
        assertThatThrownBy(() -> kp.readVarlong()).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Varlong is too long");
    }

    @Test
    void testReadTooLongUnsignedVarInt() {
        var bbuffer = ByteBuffer.allocate(1024);
        ByteBufferAccessor nio = new ByteBufferAccessor(bbuffer);
        for (int j = 0; j < 5; j++) {
            nio.writeByte((byte) -5);
        }

        var bbuf = Unpooled.wrappedBuffer(bbuffer.flip());
        var kp = new ByteBufAccessorImpl(bbuf);
        assertThatThrownBy(() -> kp.readUnsignedVarint()).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Varint is too long");
    }

    @Test
    void testReadTooLongVarInt() {
        var bbuffer = ByteBuffer.allocate(1024);
        ByteBufferAccessor nio = new ByteBufferAccessor(bbuffer);
        for (int j = 0; j < 5; j++) {
            nio.writeByte((byte) -5);
        }

        var bbuf = Unpooled.wrappedBuffer(bbuffer.flip());
        var kp = new ByteBufAccessorImpl(bbuf);
        assertThatThrownBy(() -> kp.readVarint()).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Varint is too long");
    }

    @Test
    void testReadArrayWithSizeGreaterThanRemaining() {
        var bbuffer = ByteBuffer.allocate(1);
        ByteBufferAccessor nio = new ByteBufferAccessor(bbuffer);
        nio.writeByte((byte) -5);

        var bbuf = Unpooled.wrappedBuffer(bbuffer.flip());
        var kp = new ByteBufAccessorImpl(bbuf);
        assertThatThrownBy(() -> kp.readArray(2)).isInstanceOf(IllegalArgumentException.class)
                                                 .hasMessageContaining("Error reading byte array of 2 byte(s): only 1 byte(s) available");
    }

    @Test
    void testReaderIndex() {
        var bbuf = Unpooled.buffer(2).writeZero(2);
        var kp = new ByteBufAccessorImpl(bbuf);
        assertThat(kp.readerIndex()).isZero();
        bbuf.readerIndex(1);
        assertThat(kp.readerIndex()).isEqualTo(1);
    }

    @Test
    void testWriterIndex() {
        var bbuf = Unpooled.buffer(2);
        var kp = new ByteBufAccessorImpl(bbuf);
        assertThat(kp.writerIndex()).isZero();
        bbuf.writerIndex(1);
        assertThat(kp.writerIndex()).isEqualTo(1);
    }

    @Test
    void testWriteBytes() {
        var bbuf = Unpooled.buffer(2);
        var kp = new ByteBufAccessorImpl(bbuf);

        var buf2 = Unpooled.wrappedBuffer(new byte[]{ 5, 2 });
        kp.writeBytes(buf2, 1);
        assertThat(kp.writerIndex()).isEqualTo(1);
        var nio = ByteBuffer.wrap(bbuf.array());
        var kafkaAccessor = new ByteBufferAccessor(nio);
        assertThat(kafkaAccessor.readByte()).isEqualTo((byte) 5);
    }

    @Test
    void testWriteByteBuffer() {
        var bbuf = Unpooled.buffer(2);
        var kp = new ByteBufAccessorImpl(bbuf);
        kp.writeByteBuffer(ByteBuffer.wrap(new byte[]{ 5, 2 }));
        var nio = ByteBuffer.wrap(bbuf.array());
        var kafkaAccessor = new ByteBufferAccessor(nio);
        assertThat(kafkaAccessor.readByte()).isEqualTo((byte) 5);
        assertThat(kafkaAccessor.readByte()).isEqualTo((byte) 2);
    }

    @Test
    void testReadBuffer() {
        var bytebuf = Unpooled.wrappedBuffer(new byte[]{ 5, 2 });
        var kafkaAccessor = new ByteBufAccessorImpl(bytebuf);
        assertThat(kafkaAccessor.readByteBuffer(2).array()).containsExactly((byte) 5, (byte) 2);
    }

    @Test
    void testEnsureWritableExpandsUnderlyingBufferIfAble() {
        ByteBuf buffer = Unpooled.buffer(1);
        var kp = new ByteBufAccessorImpl(buffer);
        assertThat(buffer.writableBytes()).isEqualTo(1);
        kp.ensureWritable(5);
        assertThat(buffer.writableBytes()).isGreaterThanOrEqualTo(5);
    }

    @Test
    void testWrite() {
        var bbuf = Unpooled.buffer(1024);

        Random rng = new Random();
        for (int i = 0; i < 1000; i++) {
            // Write using ByteBuffer classes
            var accessor = new ByteBufAccessorImpl(bbuf);
            byte writtenByte = (byte) rng.nextInt();
            accessor.writeByte(writtenByte);
            byte[] writtenArray = { 1, 2, 3 };
            accessor.writeByteArray(writtenArray);
            double writtenDouble = rng.nextDouble();
            accessor.writeDouble(writtenDouble);
            int writtenInt = rng.nextInt();
            accessor.writeInt(writtenInt);
            long writtenLong = rng.nextLong();
            accessor.writeLong(writtenLong);
            short writtenShort = (short) rng.nextInt();
            accessor.writeShort(writtenShort);
            int writtenUnsignedVarint = rng.nextInt();
            accessor.writeUnsignedVarint(writtenUnsignedVarint);
            int writtenVarint = rng.nextInt();
            accessor.writeVarint(writtenVarint);
            long writtenVarlong = rng.nextLong();
            accessor.writeVarlong(writtenVarlong);

            // Read using Kafka accessor
            var nio = ByteBuffer.wrap(bbuf.array());
            var kafkaAccessor = new ByteBufferAccessor(nio);
            assertEquals(writtenByte, kafkaAccessor.readByte());
            var readArray = kafkaAccessor.readArray(writtenArray.length);
            assertArrayEquals(writtenArray, readArray);
            assertEquals(writtenDouble, kafkaAccessor.readDouble());
            assertEquals(writtenInt, kafkaAccessor.readInt());
            assertEquals(writtenLong, kafkaAccessor.readLong());
            assertEquals(writtenShort, kafkaAccessor.readShort());
            assertEquals(writtenUnsignedVarint, kafkaAccessor.readUnsignedVarint());
            assertEquals(writtenVarint, kafkaAccessor.readVarint());
            assertEquals(writtenVarlong, kafkaAccessor.readVarlong());

            bbuf.clear();
        }
    }

    public static Stream<Object[]> requestApiVersions() {
        return IntStream.range(0, ApiVersionsRequestData.SCHEMAS.length)
                        .mapToObj(index -> new Object[]{ (short) (ApiVersionsRequestData.LOWEST_SUPPORTED_VERSION + index), ApiVersionsRequestData.SCHEMAS[index] });
    }

    public static Stream<Object[]> responseApiVersions() {
        return IntStream.range(0, ApiVersionsResponseData.SCHEMAS.length)
                        .mapToObj(index -> new Object[]{ (short) (ApiVersionsResponseData.LOWEST_SUPPORTED_VERSION + index), ApiVersionsResponseData.SCHEMAS[index] });
    }

    private void assertSameRequest(Schema schema, ApiVersionsRequestData message, ApiVersionsRequestData readReq) {
        assertEquals(schema.get("client_software_name") != null ? message.clientSoftwareName() : "", readReq.clientSoftwareName());
        assertEquals(schema.get("client_software_version") != null ? message.clientSoftwareVersion() : "", readReq.clientSoftwareVersion());
    }

    @ParameterizedTest
    @MethodSource("requestApiVersions")
    void testReadApiVersionsRequest(short apiVersion, Schema schema) {
        // Write using Kafka API
        var message = new ApiVersionsRequestData()
                                                  .setClientSoftwareName("foo/bar")
                                                  .setClientSoftwareVersion("1.2.0");
        var cache = new ObjectSerializationCache();
        int messageSize = message.size(cache, apiVersion);
        ByteBuffer bbuffer = ByteBuffer.allocate(messageSize);
        var kafkaAccessor = new ByteBufferAccessor(bbuffer);
        message.write(kafkaAccessor, cache, apiVersion);
        bbuffer.flip();

        // Read using our API
        var bbuf = Unpooled.wrappedBuffer(bbuffer);
        var readReq = new ApiVersionsRequestData(new ByteBufAccessorImpl(bbuf), apiVersion);

        assertSameRequest(schema, message, readReq);
    }

    @ParameterizedTest
    @MethodSource("requestApiVersions")
    void testWriteApiVersionsRequest(short apiVersion, Schema schema) {
        // Write using our API
        var message = new ApiVersionsRequestData()
                                                  .setClientSoftwareName("foo/bar")
                                                  .setClientSoftwareVersion("1.2.0");
        var cache = new ObjectSerializationCache();
        int messageSize = message.size(cache, apiVersion);
        var bbuf = Unpooled.buffer(messageSize);
        var ourAccessor = new ByteBufAccessorImpl(bbuf);
        message.write(ourAccessor, cache, apiVersion);

        // Read using the Kafka API
        var readReq = new ApiVersionsRequestData(new ByteBufferAccessor(ByteBuffer.wrap(bbuf.array())), apiVersion);
        assertSameRequest(schema, message, readReq);
    }

    private void assertSameResponse(short apiVersion, Schema schema, ApiVersionsResponseData message, ApiVersionsResponseData readReq) {
        // structural fields
        assertEquals(schema.get("error_code") != null ? message.errorCode() : (short) 0, readReq.errorCode());
        assertEquals(schema.get("throttle_time_ms") != null ? message.throttleTimeMs() : 0, readReq.throttleTimeMs());
        assertEquals(message.apiKeys(), readReq.apiKeys());
        // tagged fields
        assertEquals(apiVersion >= 3 ? message.finalizedFeaturesEpoch() : -1L, readReq.finalizedFeaturesEpoch());
        assertEquals(apiVersion >= 3 ? message.finalizedFeatures() : new ApiVersionsResponseData.FinalizedFeatureKeyCollection(), readReq.finalizedFeatures());
        assertEquals(apiVersion >= 3 ? message.supportedFeatures() : new ApiVersionsResponseData.SupportedFeatureKeyCollection(), readReq.supportedFeatures());
    }

    @ParameterizedTest
    @MethodSource("responseApiVersions")
    void testReadApiVersionsResponse(short apiVersion, Schema schema) {
        // Write using Kafka API
        var ff = new ApiVersionsResponseData.FinalizedFeatureKeyCollection();
        ff.add(
                new ApiVersionsResponseData.FinalizedFeatureKey()
                                                                 .setName("ff")
                                                                 .setMaxVersionLevel((short) 78)
                                                                 .setMinVersionLevel((short) 77)
        );
        var sf = new ApiVersionsResponseData.SupportedFeatureKeyCollection();
        sf.add(
                new ApiVersionsResponseData.SupportedFeatureKey()
                                                                 .setName("ff")
                                                                 .setMaxVersion((short) 88)
                                                                 .setMinVersion((short) 87)
        );
        var ak = new ApiVersionsResponseData.ApiVersionCollection();
        ak.add(
                new ApiVersionsResponseData.ApiVersion()
                                                        .setApiKey(ApiKeys.ADD_OFFSETS_TO_TXN.id)
                                                        .setMinVersion((short) 1)
                                                        .setMaxVersion((short) 3)
        );
        var message = new ApiVersionsResponseData()
                                                   .setErrorCode(Errors.NONE.code())
                                                   .setThrottleTimeMs(23)
                                                   .setFinalizedFeaturesEpoch(12)
                                                   .setFinalizedFeatures(ff)
                                                   .setSupportedFeatures(sf)
                                                   .setApiKeys(ak);
        var cache = new ObjectSerializationCache();
        int messageSize = message.size(cache, apiVersion);
        ByteBuffer bbuffer = ByteBuffer.allocate(messageSize);
        var kafkaAccessor = new ByteBufferAccessor(bbuffer);
        message.write(kafkaAccessor, cache, apiVersion);
        bbuffer.flip();

        // Read using our API
        var bbuf = Unpooled.wrappedBuffer(bbuffer);
        var readReq = new ApiVersionsResponseData(new ByteBufAccessorImpl(bbuf), apiVersion);

        assertSameResponse(apiVersion, schema, message, readReq);
    }

    @ParameterizedTest
    @MethodSource("responseApiVersions")
    void testWriteApiVersionsResponse(short apiVersion, Schema schema) {
        // Write using our API
        var ff = new ApiVersionsResponseData.FinalizedFeatureKeyCollection();
        ff.add(
                new ApiVersionsResponseData.FinalizedFeatureKey()
                                                                 .setName("ff")
                                                                 .setMaxVersionLevel((short) 78)
                                                                 .setMinVersionLevel((short) 77)
        );
        var sf = new ApiVersionsResponseData.SupportedFeatureKeyCollection();
        sf.add(
                new ApiVersionsResponseData.SupportedFeatureKey()
                                                                 .setName("ff")
                                                                 .setMaxVersion((short) 88)
                                                                 .setMinVersion((short) 87)
        );
        var ak = new ApiVersionsResponseData.ApiVersionCollection();
        ak.add(
                new ApiVersionsResponseData.ApiVersion()
                                                        .setApiKey(ApiKeys.ADD_OFFSETS_TO_TXN.id)
                                                        .setMinVersion((short) 1)
                                                        .setMaxVersion((short) 3)
        );
        var message = new ApiVersionsResponseData()
                                                   .setErrorCode(Errors.NONE.code())
                                                   .setThrottleTimeMs(23)
                                                   .setFinalizedFeaturesEpoch(12)
                                                   .setFinalizedFeatures(ff)
                                                   .setSupportedFeatures(sf)
                                                   .setApiKeys(ak);
        var cache = new ObjectSerializationCache();
        int messageSize = message.size(cache, apiVersion);
        var bbuf = Unpooled.buffer(messageSize);
        var ourAccessor = new ByteBufAccessorImpl(bbuf);
        message.write(ourAccessor, cache, apiVersion);

        // Read using Kafka API
        var readReq = new ApiVersionsResponseData(new ByteBufferAccessor(ByteBuffer.wrap(bbuf.array())), apiVersion);

        assertSameResponse(apiVersion, schema, message, readReq);
    }
}
