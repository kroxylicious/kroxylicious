/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.inband;

import java.nio.ByteBuffer;
import java.util.Set;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import io.kroxylicious.filter.encryption.EncryptionScheme;
import io.kroxylicious.filter.encryption.EncryptionVersion;
import io.kroxylicious.filter.encryption.RecordField;
import io.kroxylicious.test.assertj.KafkaAssertions;
import io.kroxylicious.test.record.RecordTestUtils;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

class RecordEncryptorTest {

    private Record encryptSingleRecord(Set<RecordField> fields, long offset, long timestamp, String key, String value, Header... headers) {
        var kc = mock(KeyContext.class);
        doReturn(new byte[]{ 6, 7, 8 }).when(kc).serializedEdek();
        doAnswer(invocation -> {
            ByteBuffer input = invocation.getArgument(0);
            input.position(input.limit());
            ByteBuffer output = invocation.getArgument(1);
            output.limit(output.capacity());
            return null;
        }).when(kc).encode(any(), any());
        var re = new RecordEncryptor(EncryptionVersion.V1,
                new EncryptionScheme<String>("key", fields),
                kc,
                ByteBuffer.allocate(10),
                ByteBuffer.allocate(10));

        Record record = RecordTestUtils.record(RecordBatch.MAGIC_VALUE_V2, offset, timestamp, key, value, headers);

        re.init(record);
        var tOffset = re.transformOffset(record);
        var tTimestamp = re.transformTimestamp(record);
        var tKey = re.transformKey(record);
        var tValue = re.transformValue(record);
        var tHeaders = re.transformHeaders(record);
        re.resetAfterTransform(record);

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
    }

    // TODO with legacy magic

    @Test
    void shouldEncryptValueWithExistingHeaders() {
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
                .singleHeader()
                .hasKeyEqualTo("kroxylicious.io/encryption")
                .hasValueEqualTo(new byte[]{ 1 });
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
    }

}
