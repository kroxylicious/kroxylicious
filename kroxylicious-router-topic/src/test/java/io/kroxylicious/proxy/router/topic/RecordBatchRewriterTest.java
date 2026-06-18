/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.router.topic;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceRequestData.PartitionProduceData;
import org.apache.kafka.common.message.ProduceRequestData.TopicProduceData;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.ByteBufferOutputStream;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class RecordBatchRewriterTest {

    private static final long CLIENT_PRODUCER_ID = 42L;
    private static final short CLIENT_EPOCH = 0;
    private static final long TARGET_PRODUCER_ID = 999L;
    private static final short TARGET_EPOCH = 3;

    @Test
    void shouldRewriteIdempotentBatch() {
        var request = requestWithRecords("topic", 0, CLIENT_PRODUCER_ID, CLIENT_EPOCH, 0);

        RecordBatchRewriter.rewriteProducerId(request, TARGET_PRODUCER_ID, TARGET_EPOCH);

        var rewritten = recordsFromFirstPartition(request);
        var batch = rewritten.batches().iterator().next();
        assertThat(batch.producerId()).isEqualTo(TARGET_PRODUCER_ID);
        assertThat(batch.producerEpoch()).isEqualTo(TARGET_EPOCH);
    }

    @Test
    void shouldPreserveSequenceNumbers() {
        int baseSequence = 7;
        var request = requestWithRecords("topic", 0, CLIENT_PRODUCER_ID, CLIENT_EPOCH, baseSequence);

        RecordBatchRewriter.rewriteProducerId(request, TARGET_PRODUCER_ID, TARGET_EPOCH);

        var rewritten = recordsFromFirstPartition(request);
        var batch = rewritten.batches().iterator().next();
        assertThat(batch.baseSequence()).isEqualTo(baseSequence);
    }

    @Test
    void shouldPreserveRecordContent() {
        var request = requestWithRecords("topic", 0, CLIENT_PRODUCER_ID, CLIENT_EPOCH, 0);

        RecordBatchRewriter.rewriteProducerId(request, TARGET_PRODUCER_ID, TARGET_EPOCH);

        var rewritten = recordsFromFirstPartition(request);
        var record = rewritten.records().iterator().next();
        assertThat(StandardCharsets.UTF_8.decode(record.key()).toString()).isEqualTo("key");
        assertThat(StandardCharsets.UTF_8.decode(record.value()).toString()).isEqualTo("value");
        assertThat(record.offset()).isEqualTo(0);
    }

    @Test
    void shouldNotModifyNonIdempotentBatch() {
        var request = requestWithRecords("topic", 0, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_SEQUENCE);

        MemoryRecords before = recordsFromFirstPartition(request);
        byte[] beforeBytes = new byte[before.sizeInBytes()];
        before.buffer().duplicate().get(beforeBytes);

        RecordBatchRewriter.rewriteProducerId(request, TARGET_PRODUCER_ID, TARGET_EPOCH);

        MemoryRecords after = recordsFromFirstPartition(request);
        byte[] afterBytes = new byte[after.sizeInBytes()];
        after.buffer().duplicate().get(afterBytes);
        assertThat(afterBytes).isEqualTo(beforeBytes);
    }

    @Test
    void shouldHandleNullRecords() {
        var request = new ProduceRequestData();
        var td = new TopicProduceData().setName("topic");
        td.partitionData().add(new PartitionProduceData().setIndex(0).setRecords(null));
        request.topicData().add(td);

        RecordBatchRewriter.rewriteProducerId(request, TARGET_PRODUCER_ID, TARGET_EPOCH);

        assertThat(firstPartition(request).records()).isNull();
    }

    @Test
    void shouldRewriteMultiplePartitions() {
        var request = new ProduceRequestData();
        var td = new TopicProduceData().setName("topic");
        td.partitionData().add(partitionWithRecords(0, CLIENT_PRODUCER_ID, CLIENT_EPOCH, 0));
        td.partitionData().add(partitionWithRecords(1, CLIENT_PRODUCER_ID, CLIENT_EPOCH, 5));
        request.topicData().add(td);

        RecordBatchRewriter.rewriteProducerId(request, TARGET_PRODUCER_ID, TARGET_EPOCH);

        for (var pd : request.topicData().iterator().next().partitionData()) {
            var rewritten = (MemoryRecords) pd.records();
            var batch = rewritten.batches().iterator().next();
            assertThat(batch.producerId()).isEqualTo(TARGET_PRODUCER_ID);
            assertThat(batch.producerEpoch()).isEqualTo(TARGET_EPOCH);
        }
    }

    @Test
    void shouldHandleCompressedBatch() {
        var request = requestWithCompressedRecords("topic", 0, CLIENT_PRODUCER_ID, CLIENT_EPOCH, 0);

        RecordBatchRewriter.rewriteProducerId(request, TARGET_PRODUCER_ID, TARGET_EPOCH);

        var rewritten = recordsFromFirstPartition(request);
        var batch = rewritten.batches().iterator().next();
        assertThat(batch.producerId()).isEqualTo(TARGET_PRODUCER_ID);
        assertThat(batch.producerEpoch()).isEqualTo(TARGET_EPOCH);

        var record = rewritten.records().iterator().next();
        assertThat(StandardCharsets.UTF_8.decode(record.value()).toString()).isEqualTo("value");
    }

    private static MemoryRecords recordsFromFirstPartition(ProduceRequestData request) {
        return (MemoryRecords) firstPartition(request).records();
    }

    private static PartitionProduceData firstPartition(ProduceRequestData request) {
        return request.topicData().iterator().next().partitionData().iterator().next();
    }

    // --- helpers ---

    private static ProduceRequestData requestWithRecords(String topic,
                                                         int partition,
                                                         long producerId,
                                                         short producerEpoch,
                                                         int baseSequence) {
        var request = new ProduceRequestData();
        request.setAcks((short) -1);
        request.setTimeoutMs(30000);
        var td = new TopicProduceData().setName(topic);
        td.partitionData().add(partitionWithRecords(partition, producerId, producerEpoch, baseSequence));
        request.topicData().add(td);
        return request;
    }

    private static PartitionProduceData partitionWithRecords(int partition,
                                                             long producerId,
                                                             short producerEpoch,
                                                             int baseSequence) {
        return new PartitionProduceData()
                .setIndex(partition)
                .setRecords(buildRecords(producerId, producerEpoch, baseSequence, CompressionType.NONE));
    }

    private static ProduceRequestData requestWithCompressedRecords(String topic,
                                                                   int partition,
                                                                   long producerId,
                                                                   short producerEpoch,
                                                                   int baseSequence) {
        var request = new ProduceRequestData();
        request.setAcks((short) -1);
        request.setTimeoutMs(30000);
        var td = new TopicProduceData().setName(topic);
        td.partitionData().add(new PartitionProduceData()
                .setIndex(partition)
                .setRecords(buildRecords(producerId, producerEpoch, baseSequence, CompressionType.GZIP)));
        request.topicData().add(td);
        return request;
    }

    private static MemoryRecords buildRecords(long producerId,
                                              short producerEpoch,
                                              int baseSequence,
                                              CompressionType compressionType) {
        var buf = ByteBuffer.allocate(1024);
        var builder = new MemoryRecordsBuilder(
                new ByteBufferOutputStream(buf),
                RecordBatch.CURRENT_MAGIC_VALUE,
                Compression.of(compressionType).build(),
                TimestampType.CREATE_TIME,
                0L,
                RecordBatch.NO_TIMESTAMP,
                producerId,
                producerEpoch,
                baseSequence,
                false,
                false,
                RecordBatch.NO_PARTITION_LEADER_EPOCH,
                1024);
        builder.append(new SimpleRecord(
                System.currentTimeMillis(),
                "key".getBytes(StandardCharsets.UTF_8),
                "value".getBytes(StandardCharsets.UTF_8)));
        return builder.build();
    }
}
