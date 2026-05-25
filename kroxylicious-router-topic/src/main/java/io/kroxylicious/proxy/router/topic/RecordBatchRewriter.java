/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.router.topic;

import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.ByteBufferOutputStream;

import io.kroxylicious.kafka.transform.BatchAwareMemoryRecordsBuilder;

/**
 * Rewrites the producerId and producerEpoch in record batch headers
 * within a {@link ProduceRequestData}. Used when fan-out router sends
 * sub-requests to backends that allocated their own producer IDs.
 */
final class RecordBatchRewriter {

    private RecordBatchRewriter() {
    }

    static void rewriteProducerId(ProduceRequestData subRequest,
                                  long targetProducerId,
                                  short targetProducerEpoch) {
        for (var td : subRequest.topicData()) {
            for (var pd : td.partitionData()) {
                if (pd.records() == null) {
                    continue;
                }
                MemoryRecords records = (MemoryRecords) pd.records();
                if (!hasIdempotentBatch(records)) {
                    continue;
                }
                pd.setRecords(rebuildWithProducerId(records, targetProducerId, targetProducerEpoch));
            }
        }
    }

    private static boolean hasIdempotentBatch(MemoryRecords records) {
        for (RecordBatch batch : records.batches()) {
            if (batch.producerId() != RecordBatch.NO_PRODUCER_ID) {
                return true;
            }
        }
        return false;
    }

    private static MemoryRecords rebuildWithProducerId(MemoryRecords records,
                                                       long targetProducerId,
                                                       short targetProducerEpoch) {
        var out = new ByteBufferOutputStream(records.sizeInBytes());
        var builder = new BatchAwareMemoryRecordsBuilder(out);

        for (MutableRecordBatch batch : records.batches()) {
            if (batch.producerId() != RecordBatch.NO_PRODUCER_ID) {
                TimestampType tsType = batch.timestampType();
                long logAppendTime = tsType == TimestampType.LOG_APPEND_TIME
                        ? batch.maxTimestamp()
                        : RecordBatch.NO_TIMESTAMP;
                builder.addBatch(
                        batch.magic(),
                        Compression.of(batch.compressionType()).build(),
                        tsType,
                        batch.baseOffset(),
                        logAppendTime,
                        targetProducerId,
                        targetProducerEpoch,
                        batch.baseSequence(),
                        batch.isTransactional(),
                        batch.isControlBatch(),
                        batch.partitionLeaderEpoch(),
                        batch.deleteHorizonMs().orElse(RecordBatch.NO_TIMESTAMP));
                for (Record record : batch) {
                    builder.appendWithOffset(record.offset(), record);
                }
            }
            else {
                builder.writeBatch(batch);
            }
        }

        return builder.build();
    }
}
