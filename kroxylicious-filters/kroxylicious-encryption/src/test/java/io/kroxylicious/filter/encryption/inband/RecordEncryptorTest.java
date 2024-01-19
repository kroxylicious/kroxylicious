/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.inband;

import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Set;

import javax.crypto.KeyGenerator;

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
import io.kroxylicious.filter.encryption.records.RecordTransform;
import io.kroxylicious.test.assertj.KafkaAssertions;
import io.kroxylicious.test.record.RecordTestUtils;

class RecordEncryptorTest {

    private static AesGcmEncryptor DECRYPTOR;
    private static KeyContext KEY_CONTEXT;
    private static ByteBuffer EDEK;

    @BeforeAll
    public static void initKeyContext() throws NoSuchAlgorithmException {
        var generator = KeyGenerator.getInstance("AES");
        var key = generator.generateKey();
        EDEK = ByteBuffer.wrap(key.getEncoded()); // it doesn't matter for this test that it's not encrypted
        AesGcmEncryptor encryptor = AesGcmEncryptor.forEncrypt(new AesGcmIvGenerator(new SecureRandom()), key);
        DECRYPTOR = AesGcmEncryptor.forDecrypt(key);
        KEY_CONTEXT = new KeyContext(EDEK, Long.MAX_VALUE, Integer.MAX_VALUE, encryptor);
    }

    private Record encryptSingleRecord(Set<RecordField> fields, long offset, long timestamp, String key, String value, Header... headers) {
        var kc = KEY_CONTEXT;
        var re = new RecordEncryptor(EncryptionVersion.V1,
                new EncryptionScheme<>("key", fields),
                kc,
                ByteBuffer.allocate(100),
                ByteBuffer.allocate(100));

        Record record = RecordTestUtils.record(RecordBatch.MAGIC_VALUE_V2, offset, timestamp, key, value, headers);

        return transformRecord(re, record);
    }

    private Record transformRecord(RecordTransform recordTransform, Record record) {
        recordTransform.init(record);
        var tOffset = recordTransform.transformOffset(record);
        var tTimestamp = recordTransform.transformTimestamp(record);
        var tKey = recordTransform.transformKey(record);
        var tValue = recordTransform.transformValue(record);
        var tHeaders = recordTransform.transformHeaders(record);
        recordTransform.resetAfterTransform(record);
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
        var rd = new RecordDecryptor(index -> new DecryptState(t, EncryptionVersion.V1, DECRYPTOR));
        var rt = transformRecord(rd, t);

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
        var rd = new RecordDecryptor(index -> new DecryptState(t, EncryptionVersion.V1, DECRYPTOR));
        var rt = transformRecord(rd, t);

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
        var rd = new RecordDecryptor(index -> null); // note the null return
        var rt = transformRecord(rd, t);

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
        var rd = new RecordDecryptor(index -> new DecryptState(t, EncryptionVersion.V1, DECRYPTOR));
        var rt = transformRecord(rd, t);

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
