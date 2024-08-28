/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.encrypt;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import io.kroxylicious.filter.encryption.FixedDekKmsService;
import io.kroxylicious.filter.encryption.config.RecordField;
import io.kroxylicious.filter.encryption.crypto.Encryption;
import io.kroxylicious.filter.encryption.decrypt.DecryptState;
import io.kroxylicious.filter.encryption.decrypt.RecordDecryptor;
import io.kroxylicious.filter.encryption.dek.Aes;
import io.kroxylicious.filter.encryption.dek.Dek;
import io.kroxylicious.filter.encryption.dek.DekManager;
import io.kroxylicious.kafka.transform.RecordTransform;
import io.kroxylicious.kms.service.Serde;
import io.kroxylicious.test.assertj.KafkaAssertions;
import io.kroxylicious.test.record.RecordTestUtils;

import static org.mockito.Mockito.mock;

class RecordEncryptorTest {

    private static TestComponents components;
    private static FixedDekKmsService fixedDekKmsService;

    record TestComponents(
                          Dek.Encryptor encryptor,
                          Dek.Decryptor decryptor,
                          Serde<ByteBuffer> edekSerde)
            implements Closeable {
        public void close() {
            encryptor.close();
            decryptor.close();
        }
    }

    @BeforeAll
    public static void initKeyContext() {
        components = setup(256);
    }

    @AfterAll
    public static void teardown() {
        components.close();
    }

    static TestComponents setup(int keysize) {
        try {
            fixedDekKmsService = new FixedDekKmsService(keysize);
            var kms = fixedDekKmsService.buildKms();
            DekManager<ByteBuffer, ByteBuffer> manager = new DekManager<>(kms, 10000);
            CompletionStage<Dek<ByteBuffer>> dekCompletionStage = manager.generateDek(fixedDekKmsService.getKekId(), Aes.AES_256_GCM_128);
            Dek<ByteBuffer> dek = dekCompletionStage.toCompletableFuture().get(0, TimeUnit.SECONDS);
            var encryptor = dek.encryptor(1000);
            var decryptor = dek.decryptor();
            return new TestComponents(encryptor, decryptor, fixedDekKmsService.edekSerde());
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Record encryptSingleRecord(TestComponents components, Set<RecordField> fields, long offset, long timestamp, String key, String value, Header... headers) {
        var re = new RecordEncryptor(
                "topic", 0,
                Encryption.V2,
                new EncryptionScheme<>("key", fields),
                components.edekSerde,
                ByteBuffer.allocate(100));

        Record record = RecordTestUtils.record(RecordBatch.MAGIC_VALUE_V2, offset, timestamp, key, value, headers);

        return transformRecord(re, components.encryptor, record);
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
        var t = encryptSingleRecord(components, fields, offset, timestamp, key, value);

        // Then
        KafkaAssertions.assertThat(t)
                .hasOffsetEqualTo(offset)
                .hasTimestampEqualTo(timestamp)
                .hasKeyEqualTo(key)
                .hasValueNotEqualTo(value)
                .singleHeader()
                .hasKeyEqualTo("kroxylicious.io/encryption")
                .hasValueEqualTo(new byte[]{ 2 });

        // And when
        var rd = new RecordDecryptor("topic", 0);
        var rt = transformRecord(rd, new DecryptState(Encryption.V2).withDecryptor(components.decryptor), t);

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
        var t = encryptSingleRecord(components, fields, offset, timestamp, key, value, header);

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

        var rt = transformRecord(rd, new DecryptState(Encryption.V2).withDecryptor(components.decryptor), t);

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
        var t = encryptSingleRecord(components, fields, offset, timestamp, key, value);

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
        var t = encryptSingleRecord(components, fields, offset, timestamp, key, value, header);

        // Then
        KafkaAssertions.assertThat(t)
                .hasOffsetEqualTo(offset)
                .hasTimestampEqualTo(timestamp)
                .hasKeyEqualTo(key)
                .hasValueNotEqualTo(value)
                .singleHeader()
                .hasKeyEqualTo("kroxylicious.io/encryption")
                .hasValueEqualTo(new byte[]{ 2 });

        // And when
        var rd = new RecordDecryptor("topic", 0);
        var rt = transformRecord(rd, new DecryptState<>(Encryption.V2).withDecryptor(components.decryptor), t);

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
        var t = encryptSingleRecord(components, fields, offset, timestamp, key, value, header);

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
        var t = encryptSingleRecord(components, fields, offset, timestamp, key, value, header);

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
        var t = encryptSingleRecord(components, fields, offset, timestamp, key, value, header);

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
