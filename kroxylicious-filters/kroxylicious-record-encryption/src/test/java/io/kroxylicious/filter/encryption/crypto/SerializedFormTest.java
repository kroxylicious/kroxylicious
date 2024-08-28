/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.crypto;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.EnumSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.utils.ByteUtils;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

import io.kroxylicious.filter.encryption.config.AadSpec;
import io.kroxylicious.filter.encryption.config.RecordField;
import io.kroxylicious.filter.encryption.dek.CipherSpecResolver;
import io.kroxylicious.filter.encryption.dek.DekManager;
import io.kroxylicious.filter.encryption.dek.NullCipherManager;
import io.kroxylicious.kms.service.DekPair;
import io.kroxylicious.kms.service.DestroyableRawSecretKey;
import io.kroxylicious.kms.service.Kms;
import io.kroxylicious.kms.service.KmsService;
import io.kroxylicious.kms.service.Serde;
import io.kroxylicious.test.record.RecordTestUtils;

import edu.umd.cs.findbugs.annotations.NonNull;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doReturn;

/**
 * Tests that the serialization done by wrapper and parcel are what we'd expect,
 * that they can cope with the records and recordFields we expect
 * and that they can be combined together in the expected way
 */
class SerializedFormTest {

    // TODO abstract resolver validates names and ids

    /*
     * wrapper_v2 = cipher_id
     * edek_length
     * edek
     * aad_id
     * [ cipher_parameters_length ] ; iff {@link CipherManager#constantParamsSize()} returns -1
     * cipher_parameters
     * parcel_ciphertext
     */
    @NonNull
    private static String expectedWrapperV2Hex(byte cipherId,
                                               String edekHex,
                                               byte aadId,
                                               String cipherParamsHex,
                                               RecordBatch batch,
                                               Set<RecordField> recordFields,
                                               Parcel parcel) {
        return asHex(cipherId) // cipher_id
                + unsignedVarintAsHex(edekHex.length() / 2) // edek_length
                + edekHex // edek
                + asHex(aadId) // aad_id
                // cipher_parameters_length omitted because constant size
                + cipherParamsHex // cipher_parameters
                + parcelHex(parcel, recordFields, batch.iterator().next()); // parcel_ciphertext
    }

    private static String parcelHex(Parcel parcel, Set<RecordField> recordFields, Record record) {
        var parcelSize = parcel.sizeOfParcel(recordFields, record);
        var parcelBuffer = ByteBuffer.allocate(parcelSize);
        parcel.writeParcel(recordFields, record, parcelBuffer);
        parcelBuffer.flip();
        String parcelHex = HexFormat.of().formatHex(parcelBuffer.array());
        return parcelHex;
    }

    private static RecordBatch recordBatch(String recordValueHex, Header[] headers) {
        final byte[] recordValueBytes = HexFormat.of().parseHex(recordValueHex);
        RecordBatch batch = RecordTestUtils.singleElementMemoryRecords(
                headers == null ? RecordBatch.MAGIC_VALUE_V1 : RecordTestUtils.DEFAULT_MAGIC_VALUE,
                RecordTestUtils.DEFAULT_OFFSET,
                RecordTestUtils.DEFAULT_TIMESTAMP,
                "hello".getBytes(StandardCharsets.UTF_8),
                recordValueBytes,
                headers).firstBatch();
        return batch;
    }

    static List<Arguments> wrapperV2SerializedForm() {
        Header[] nonEmptyHeaders = { new RecordHeader("my-header", HexFormat.of().parseHex("4eade7")) };
        Header[] emptyHeaders = new Header[0];
        EnumSet<RecordField> justValue = EnumSet.of(RecordField.RECORD_VALUE);
        EnumSet<RecordField> valueAndHeaders = EnumSet.of(RecordField.RECORD_VALUE, RecordField.RECORD_HEADER_VALUES);
        return List.of(
                Arguments.of(justValue, "cafebabe", null),
                Arguments.of(valueAndHeaders, "cafebabe", null),
                Arguments.of(justValue, "cafebabe", emptyHeaders),
                Arguments.of(valueAndHeaders, "cafebabe", emptyHeaders),
                Arguments.of(justValue, "cafebabe", nonEmptyHeaders),
                Arguments.of(valueAndHeaders, "cafebabe", nonEmptyHeaders));
    }

    @ParameterizedTest
    @MethodSource("wrapperV2SerializedForm")
    void parcelV1SerializedForm(EnumSet<RecordField> recordFields, String recordValueHex, Header[] headers) {
        // given
        RecordBatch batch = recordBatch(recordValueHex, headers);
        String expectedHeadersHex;
        if (recordFields.contains(RecordField.RECORD_HEADER_VALUES)) {
            if (headers == null) {
                expectedHeadersHex = varintAsHex(0); // TODO It seems that headers can never be null? but parcel V1 has (dead) code to use NULL_MARKER
            }
            else {
                expectedHeadersHex = varintAsHex(headers.length);
                for (var header : headers) {
                    expectedHeadersHex += unsignedVarintAsHex(header.key().length());
                    expectedHeadersHex += HexFormat.of().formatHex(header.key().getBytes(StandardCharsets.UTF_8));
                    expectedHeadersHex += varintAsHex(header.value().length);
                    expectedHeadersHex += HexFormat.of().formatHex(header.value());
                }
            }
        }
        else {
            expectedHeadersHex = varintAsHex(ParcelV1.ABSENT_MARKER);
        }
        String expectedParcelHex = varintAsHex(recordValueHex.length() / 2)
                + recordValueHex
                + expectedHeadersHex;
        // when
        String parcelV1Hex = parcelHex(ParcelV1.INSTANCE, recordFields, batch.iterator().next());
        // then
        assertThat(parcelV1Hex).isEqualTo(expectedParcelHex);
    }

    // that WrapperV2 has the expected serial form
    @ParameterizedTest
    @MethodSource
    void wrapperV2SerializedForm(EnumSet<RecordField> recordFields, String recordValueHex, Header[] headers) {
        String cipherParamsHex = "deadbeef";
        final byte cipherId = 0x17;
        boolean constantParamsSize = true;
        final byte aadId = 0x05;
        byte[] kekId = HexFormat.of().parseHex("0cec1d");
        String edekHex = "edec";
        ParcelV1 parcel = ParcelV1.INSTANCE;
        RecordBatch batch = recordBatch(recordValueHex, headers);

        String expectedHex = expectedWrapperV2Hex(cipherId, edekHex, aadId, cipherParamsHex, batch, recordFields, parcel);
        assertWrapperV2SerializedForm(cipherParamsHex, cipherId, constantParamsSize, recordFields, batch, aadId, edekHex, kekId, parcel, expectedHex);
    }

    private void assertWrapperV2SerializedForm(String cipherParamsHex,
                                               byte cipherId,
                                               boolean constantParamsSize,
                                               EnumSet<RecordField> recordFields,
                                               RecordBatch batch,
                                               byte aadId,
                                               String edekHex,
                                               byte[] kekId,
                                               Parcel parcel,
                                               String expected) {
        Record record = batch.iterator().next();

        var topicName = "my-topic";
        var partitionId = 42;
        final byte[] cipherParamsBytes = HexFormat.of().parseHex(cipherParamsHex);
        NullCipherManager cm = new NullCipherManager(cipherId, constantParamsSize, cipherParamsBytes);
        Aad aad = new MyAad(aadId);

        Kms<byte[], byte[]> kms = Mockito.mock(Kms.class);
        DekPair dekPair = new DekPair(HexFormat.of().parseHex(edekHex), DestroyableRawSecretKey.takeOwnershipOf(HexFormat.of().parseHex("0dec"), "foo"));
        doReturn(CompletableFuture.completedFuture(dekPair)).when(kms).generateDekPair(kekId);
        doReturn(new ByteArraySerde()).when(kms).edekSerde();
        KmsService<Object, byte[], byte[]> kmsService = Mockito.mock(KmsService.class);
        doReturn(kms).when(kmsService).buildKms();
        var dm = new DekManager<>(kms, 1);

        var dek = dm.generateDek(kekId, cm).toCompletableFuture().join();
        byte[] edek = dek.edek();
        Serde<byte[]> edekSerde = dm.edekSerde();

        WrapperV2 wrapper = new WrapperV2(new CipherSpecResolver(List.of(cm)),
                new AadResolver(List.of(aad)));

        final int edekLength = edekSerde.sizeOf(edek);
        final var edekBuffer = ByteBuffer.allocate(edekLength);
        edekSerde.serialize(edek, edekBuffer);
        edekBuffer.flip().asReadOnlyBuffer();
        var ct = record.value();

        // when
        var buffer = ByteBuffer.allocate(100);

        wrapper.writeWrapper(edekSerde,
                edek,
                topicName,
                partitionId,
                batch,
                record,
                dek.encryptor(1),
                parcel,
                aad,
                recordFields,
                buffer);
        buffer.flip();

        // then
        assertThat(RecordTestUtils.asHex(buffer)).isEqualTo(
                expected // parcel_ciphertext: absent headers
        );
    }

    private static String asHex(byte aadId) {
        return HexFormat.of().formatHex(new byte[]{ aadId });
    }

    private static String varintAsHex(int i) {
        var bb = ByteBuffer.allocate(ByteUtils.sizeOfVarint(i));
        ByteUtils.writeVarint(i, bb);
        return RecordTestUtils.asHex(bb.flip());
    }

    private static String unsignedVarintAsHex(int i) {
        var bb = ByteBuffer.allocate(ByteUtils.sizeOfUnsignedVarint(i));
        ByteUtils.writeUnsignedVarint(i, bb);
        return RecordTestUtils.asHex(bb.flip());
    }

    static class ByteArraySerde implements Serde<byte[]> {

        @Override
        public int sizeOf(byte[] object) {
            return object.length;
        }

        @Override
        public void serialize(
                              byte[] object,
                              @NonNull ByteBuffer buffer) {
            buffer.put(object);
        }

        @Override
        public byte[] deserialize(@NonNull ByteBuffer buffer) {
            return buffer.array();
        }
    }

    private static class MyAad implements Aad {
        private final byte aadId;

        MyAad(byte aadId) {
            this.aadId = aadId;
        }

        // a fake AAD so that we can make the serializedId something less common than 0 in the output
        @Override
        public ByteBuffer computeAad(
                                     String topicName,
                                     int partitionId,
                                     RecordBatch batch) {
            return ByteUtils.EMPTY_BUF;
        }

        @Override
        public byte serializedId() {
            return aadId;
        }

        @Override
        public AadSpec name() {
            return AadSpec.NONE;
        }
    }
}
