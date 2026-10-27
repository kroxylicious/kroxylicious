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
package io.kroxylicious.kafka.common.record.internal;

import java.io.DataOutputStream;
import java.nio.ByteBuffer;

import org.junit.jupiter.api.Test;

import io.kroxylicious.kafka.common.compress.Compression;
import io.kroxylicious.kafka.common.header.Header;
import io.kroxylicious.kafka.common.record.TimestampType;
import io.kroxylicious.kafka.common.utils.BufferSupplier;
import io.kroxylicious.kafka.common.utils.ByteBufferInputStream;
import io.kroxylicious.kafka.common.utils.ByteBufferOutputStream;
import io.kroxylicious.kafka.common.utils.CloseableIterator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for backwards-compatible API overloads retained for binary compatibility with callers
 * compiled against 0.24.x.
 */
@SuppressWarnings("removal")
public class BackwardsCompatApiTest {

    @Test
    public void deprecatedFilterToProducesEquivalentResult() {
        // Given
        ByteBuffer buffer = ByteBuffer.allocate(2048);
        MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, RecordBatch.MAGIC_VALUE_V2,
                Compression.NONE, TimestampType.CREATE_TIME, 0L);
        builder.append(10L, "key".getBytes(), "value".getBytes());
        MemoryRecords records = builder.build();
        ByteBuffer filtered3 = ByteBuffer.allocate(2048);
        ByteBuffer filtered4 = ByteBuffer.allocate(2048);
        MemoryRecords.RecordFilter retainAll = new MemoryRecords.RecordFilter(0, 0) {
            @Override
            protected MemoryRecords.RecordFilter.BatchRetentionResult checkBatchRetention(RecordBatch batch) {
                return new BatchRetentionResult(MemoryRecords.RecordFilter.BatchRetention.DELETE_EMPTY, false);
            }

            @Override
            public boolean shouldRetainRecord(RecordBatch batch, Record record) {
                return true;
            }
        };

        // When
        MemoryRecords.FilterResult result3 = records.filterTo(retainAll, filtered3, BufferSupplier.NO_CACHING);
        MemoryRecords.FilterResult result4 = records.filterTo(retainAll, filtered4, BufferSupplier.NO_CACHING, Records.SOFT_MAX_ARRAY_LENGTH);

        // Then
        assertEquals(result4.messagesRead(), result3.messagesRead());
        assertEquals(result4.messagesRetained(), result3.messagesRetained());
        assertEquals(result4.bytesRetained(), result3.bytesRetained());
    }

    @Test
    public void deprecatedOffsetOfMaxTimestampProducesEquivalentResult() {
        // Given
        ByteBuffer buffer = ByteBuffer.allocate(2048);
        MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, RecordBatch.MAGIC_VALUE_V2,
                Compression.NONE, TimestampType.CREATE_TIME, 0L);
        builder.append(10L, "key".getBytes(), "value".getBytes());
        builder.append(20L, "key2".getBytes(), "value2".getBytes());
        MemoryRecords records = builder.build();
        RecordBatch batch = records.batches().iterator().next();

        // When
        var result0Arg = batch.offsetOfMaxTimestamp();
        var result1Arg = batch.offsetOfMaxTimestamp(Records.SOFT_MAX_ARRAY_LENGTH);

        // Then
        assertEquals(result1Arg, result0Arg);
    }

    @Test
    public void defaultStreamingIteratorDelegatesToSingleArg() {
        // Given
        @SuppressWarnings("unchecked")
        CloseableIterator<Record> sentinel = mock(CloseableIterator.class);
        BufferSupplier supplier = BufferSupplier.NO_CACHING;
        RecordBatch batch = mock(RecordBatch.class);
        when(batch.streamingIterator(supplier)).thenReturn(sentinel);
        when(batch.streamingIterator(any(), anyInt())).thenCallRealMethod();

        // When
        CloseableIterator<Record> result = batch.streamingIterator(supplier, Records.SOFT_MAX_ARRAY_LENGTH);

        // Then
        assertThat(result).isSameAs(sentinel);
        verify(batch).streamingIterator(supplier);
    }

    @Test
    public void defaultSkipKeyValueIteratorDelegatesToSingleArg() {
        // Given
        @SuppressWarnings("unchecked")
        CloseableIterator<Record> sentinel = mock(CloseableIterator.class);
        BufferSupplier supplier = BufferSupplier.NO_CACHING;
        MutableRecordBatch batch = mock(MutableRecordBatch.class);
        when(batch.skipKeyValueIterator(supplier)).thenReturn(sentinel);
        when(batch.skipKeyValueIterator(any(), anyInt())).thenCallRealMethod();

        // When
        CloseableIterator<Record> result = batch.skipKeyValueIterator(supplier, Records.SOFT_MAX_ARRAY_LENGTH);

        // Then
        assertThat(result).isSameAs(sentinel);
        verify(batch).skipKeyValueIterator(supplier);
    }

    @Test
    public void deprecatedReadFromStreamProducesEquivalentResult() throws Exception {
        // Given
        ByteBuffer serialized = serializeRecord();
        long baseOffset = 37L;
        long baseTimestamp = 123L;

        // When
        DefaultRecord result5;
        try (ByteBufferInputStream in = new ByteBufferInputStream(serialized.duplicate())) {
            result5 = DefaultRecord.readFrom(in, baseOffset, baseTimestamp, RecordBatch.NO_SEQUENCE, null);
        }
        DefaultRecord result6;
        try (ByteBufferInputStream in = new ByteBufferInputStream(serialized.duplicate())) {
            result6 = DefaultRecord.readFrom(in, baseOffset, baseTimestamp, RecordBatch.NO_SEQUENCE, null, Records.SOFT_MAX_ARRAY_LENGTH);
        }

        // Then
        assertEquals(result6, result5);
    }

    @Test
    public void deprecatedReadPartiallyFromStreamProducesEquivalentResult() throws Exception {
        // Given
        ByteBuffer serialized = serializeRecord();
        long baseOffset = 37L;
        long baseTimestamp = 123L;

        // When
        PartialDefaultRecord result5;
        try (ByteBufferInputStream in = new ByteBufferInputStream(serialized.duplicate())) {
            result5 = DefaultRecord.readPartiallyFrom(in, baseOffset, baseTimestamp, RecordBatch.NO_SEQUENCE, null);
        }
        PartialDefaultRecord result6;
        try (ByteBufferInputStream in = new ByteBufferInputStream(serialized.duplicate())) {
            result6 = DefaultRecord.readPartiallyFrom(in, baseOffset, baseTimestamp, RecordBatch.NO_SEQUENCE, null, Records.SOFT_MAX_ARRAY_LENGTH);
        }

        // Then
        assertEquals(result6, result5);
    }

    private static ByteBuffer serializeRecord() throws java.io.IOException {
        ByteBuffer key = ByteBuffer.wrap("key".getBytes());
        ByteBuffer value = ByteBuffer.wrap("value".getBytes());
        ByteBufferOutputStream out = new ByteBufferOutputStream(1024);
        DefaultRecord.writeTo(new DataOutputStream(out), 10, 323L, key, value, new Header[0]);
        ByteBuffer buffer = out.buffer();
        buffer.flip();
        return buffer;
    }
}
