/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.inband;

import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.util.Set;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import io.kroxylicious.filter.encryption.EncryptionScheme;
import io.kroxylicious.filter.encryption.EncryptionVersion;
import io.kroxylicious.filter.encryption.RecordField;
import io.kroxylicious.filter.encryption.dek.CipherSpec;
import io.kroxylicious.filter.encryption.dek.Dek;
import io.kroxylicious.filter.encryption.records.RecordTransform;
import io.kroxylicious.kms.service.Serde;
import io.kroxylicious.test.assertj.KafkaAssertions;
import io.kroxylicious.test.record.RecordTestUtils;

import edu.umd.cs.findbugs.annotations.NonNull;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

class RecordEncryptorTest {

    private static Dek.Decryptor DECRYPTOR;

    private static Dek.Encryptor ENCRYPTOR;
    private static ByteBuffer EDEK;
    private static Serde<ByteBuffer> EDEK_SERDE;

    @BeforeAll
    public static void initKeyContext() throws NoSuchAlgorithmException, ReflectiveOperationException {
        var generator = KeyGenerator.getInstance("AES");
        var key = generator.generateKey();
        EDEK = ByteBuffer.wrap(key.getEncoded()); // it doesn't matter for this test that it's not encrypted

        var dek = mock(Dek.class);
        doReturn(EDEK).when(dek).edek();
        var encryptorConstructor = Dek.Encryptor.class.getDeclaredConstructor(Dek.class, CipherSpec.class, SecretKey.class, Integer.TYPE);
        encryptorConstructor.setAccessible(true);
        ENCRYPTOR = encryptorConstructor.newInstance(dek, CipherSpec.AES_128_GCM_128, key, 1_000_000);
        doReturn(ENCRYPTOR).when(dek).encryptor(anyInt());

        var decryptorConstructor = Dek.Decryptor.class.getDeclaredConstructor(Dek.class, CipherSpec.class, SecretKey.class);
        decryptorConstructor.setAccessible(true);
        DECRYPTOR = decryptorConstructor.newInstance(dek, CipherSpec.AES_128_GCM_128, key);
        doReturn(DECRYPTOR).when(dek).decryptor();

        EDEK_SERDE = new Serde<ByteBuffer>() {
            @Override
            public int sizeOf(ByteBuffer object) {
                return object.remaining();
            }

            @Override
            public void serialize(
                                  ByteBuffer object,
                                  @NonNull ByteBuffer buffer) {
                var p0 = object.position();
                buffer.put(object);
                object.position(p0);
            }

            @Override
            public ByteBuffer deserialize(@NonNull ByteBuffer buffer) {
                throw new UnsupportedOperationException();
            }
        };
    }

    private Record encryptSingleRecord(Set<RecordField> fields, long offset, long timestamp, String key, String value, Header... headers) {
        var re = new RecordEncryptor(
                "topic", 0,
                EncryptionVersion.V1,
                new EncryptionScheme<>("key", fields),
                EDEK_SERDE,
                ByteBuffer.allocate(100));

        Record record = RecordTestUtils.record(RecordBatch.MAGIC_VALUE_V2, offset, timestamp, key, value, headers);

        return transformRecord(re, ENCRYPTOR, record);
    }

    private <S> Record transformRecord(RecordTransform<S> recordTransform, S state, Record record) {
        recordTransform.initBatch(mock(RecordBatch.class));
        recordTransform.init(state, record);
        var tOffset = recordTransform.transformOffset(record);
        var tTimestamp = recordTransform.transformTimestamp(record);
        var tKey = recordTransform.transformKey(record);
        var tValue = recordTransform.transformValue(record);
        var tHeaders = recordTransform.transformHeaders(record);
        recordTransform.resetAfterTransform(state, record);
        return RecordTestUtils.record(tOffset, tTimestamp, tKey, tValue, tHeaders);
    }

    @Test
    void shouldEncryptValueOnlyWithNoExistingHeaders() {
        // Given
        Set<RecordField> fields = Set.of(RecordField.RECORD_VALUE);
        long offset = 55L;
        long timestamp = System.currentTimeMillis();
        var key = "hello";
        var value = "world";

        // When
        var t = encryptSingleRecord(fields, offset, timestamp, key, value);

        // Then
        KafkaAssertions.assertThat(t)
                .hasOffsetEqualTo(offset)
                .hasTimestampEqualTo(timestamp)
                .hasKeyEqualTo(key)
                .hasValueNotEqualTo(value)
                .singleHeader()
                .hasKeyEqualTo("kroxylicious.io/encryption")
                .hasValueEqualTo(new byte[]{ 1 });

        // And when
        var rd = new RecordDecryptor("topic", 0);
        var rt = transformRecord(rd, new DecryptState(EncryptionVersion.V1).withDecryptor(DECRYPTOR), t);

        // Then
        KafkaAssertions.assertThat(rt)
                .hasOffsetEqualTo(offset)
                .hasTimestampEqualTo(timestamp)
                .hasKeyEqualTo(key)
                .hasValueEqualTo(value)
                .hasEmptyHeaders();
    }

    // TODO with legacy magic

    @Test
    void shouldEncryptValueOnlyWithExistingHeaders() {
        // Given
        Set<RecordField> fields = Set.of(RecordField.RECORD_VALUE);
        long offset = 55L;
        long timestamp = System.currentTimeMillis();
        var key = "hello";
        var value = "world";
        var header = new RecordHeader("bob", null);

        // When
        var t = encryptSingleRecord(fields, offset, timestamp, key, value, header);

        // Then
        KafkaAssertions.assertThat(t)
                .hasOffsetEqualTo(offset)
                .hasTimestampEqualTo(timestamp)
                .hasKeyEqualTo(key)
                .hasValueNotEqualTo(value)
                .hasHeadersSize(2)
                .containsHeaderWithKey("kroxylicious.io/encryption")
                .containsHeaderWithKey("bob");

        // And when
        var rd = new RecordDecryptor("topic", 0); // index -> new DecryptState(t, EncryptionVersion.V1, DECRYPTOR)

        var rt = transformRecord(rd, new DecryptState(EncryptionVersion.V1).withDecryptor(DECRYPTOR), t);

        // Then
        KafkaAssertions.assertThat(rt)
                .hasOffsetEqualTo(offset)
                .hasTimestampEqualTo(timestamp)
                .hasKeyEqualTo(key)
                .hasValueEqualTo(value)
                .singleHeader()
                .hasKeyEqualTo("bob")
                .hasNullValue();
    }

    @Test
    void shouldEncryptValueOnlyPreservesNullValue() {
        // Given
        Set<RecordField> fields = Set.of(RecordField.RECORD_VALUE);
        long offset = 55L;
        long timestamp = System.currentTimeMillis();
        var key = "hello";
        var value = (String) null;

        // When
        var t = encryptSingleRecord(fields, offset, timestamp, key, value);

        // Then
        KafkaAssertions.assertThat(t)
                .hasOffsetEqualTo(offset)
                .hasTimestampEqualTo(timestamp)
                .hasKeyEqualTo(key)
                .hasNullValue()
                .hasEmptyHeaders();

        // And when
        var rd = new RecordDecryptor("topic", 0); // note the null return
        var rt = transformRecord(rd, null, t);

        // Then
        KafkaAssertions.assertThat(rt)
                .hasOffsetEqualTo(offset)
                .hasTimestampEqualTo(timestamp)
                .hasKeyEqualTo(key)
                .hasNullValue()
                .hasEmptyHeaders();
    }

    @Test
    void shouldEncryptValueAndHeaders() {
        // Given
        Set<RecordField> fields = Set.of(RecordField.RECORD_VALUE, RecordField.RECORD_HEADER_VALUES);
        long offset = 55L;
        long timestamp = System.currentTimeMillis();
        var key = "hello";
        var value = "world";
        var header = new RecordHeader("bob", null);

        // When
        var t = encryptSingleRecord(fields, offset, timestamp, key, value, header);

        // Then
        KafkaAssertions.assertThat(t)
                .hasOffsetEqualTo(offset)
                .hasTimestampEqualTo(timestamp)
                .hasKeyEqualTo(key)
                .hasValueNotEqualTo(value)
                .singleHeader()
                .hasKeyEqualTo("kroxylicious.io/encryption")
                .hasValueEqualTo(new byte[]{ 1 });

        // And when
        var rd = new RecordDecryptor("topic", 0);
        var rt = transformRecord(rd, new DecryptState<>(EncryptionVersion.V1).withDecryptor(DECRYPTOR), t);

        // Then
        KafkaAssertions.assertThat(rt)
                .hasOffsetEqualTo(offset)
                .hasTimestampEqualTo(timestamp)
                .hasKeyEqualTo(key)
                .hasValueEqualTo(value)
                .singleHeader()
                .hasKeyEqualTo("bob")
                .hasNullValue();
    }

    @Test
    @Disabled("Not implemented yet")
    void shouldEncryptValueAndHeadersPreservesNullValue() {
        // Given
        Set<RecordField> fields = Set.of(RecordField.RECORD_VALUE, RecordField.RECORD_HEADER_VALUES);
        long offset = 55L;
        long timestamp = System.currentTimeMillis();
        var key = "hello";
        var value = (String) null;
        var header = new RecordHeader("bob", null);

        // When
        var t = encryptSingleRecord(fields, offset, timestamp, key, value, header);

        // Then
        KafkaAssertions.assertThat(t)
                .hasOffsetEqualTo(offset)
                .hasTimestampEqualTo(timestamp)
                .hasKeyEqualTo(key)
                .hasNullValue()
                .singleHeader()
                .hasKeyEqualTo("kroxylicious.io/encryption")
                .hasValueEqualTo(new byte[]{ 1 });

        // TODO decryption
    }

    @Test
    @Disabled("Not supported yet")
    void shouldEncryptHeadersOnly() {
        // Given
        Set<RecordField> fields = Set.of(RecordField.RECORD_HEADER_VALUES);
        long offset = 55L;
        long timestamp = System.currentTimeMillis();
        var key = "hello";
        var value = "world";
        var header = new RecordHeader("bob", null);

        // When
        var t = encryptSingleRecord(fields, offset, timestamp, key, value, header);

        // Then
        KafkaAssertions.assertThat(t)
                .hasOffsetEqualTo(offset)
                .hasTimestampEqualTo(timestamp)
                .hasKeyEqualTo(key)
                .hasValueEqualTo(value)
                .singleHeader()
                .hasKeyEqualTo("kroxylicious.io/encryption")
                .hasValueEqualTo(new byte[]{ 1 });

        // TODO decryption
    }

    @Test
    @Disabled("Not supported yet")
    void shouldEncryptHeadersOnlyPreservesNullValue() {
        // Given
        Set<RecordField> fields = Set.of(RecordField.RECORD_HEADER_VALUES);
        long offset = 55L;
        long timestamp = System.currentTimeMillis();
        var key = "hello";
        var value = (String) null;
        var header = new RecordHeader("bob", null);

        // When
        var t = encryptSingleRecord(fields, offset, timestamp, key, value, header);

        // Then
        KafkaAssertions.assertThat(t)
                .hasOffsetEqualTo(offset)
                .hasTimestampEqualTo(timestamp)
                .hasKeyEqualTo(key)
                .hasValueEqualTo(value)
                .singleHeader()
                .hasKeyEqualTo("kroxylicious.io/encryption")
                .hasValueEqualTo(new byte[]{ 1 });

        // TODO decryption
    }

}
