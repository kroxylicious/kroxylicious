/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.util;

import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.ByteBufferOutputStream;

/**
 * This introduces additional factory builder methods for {@link org.apache.kafka.common.record.MemoryRecords} that
 * accepts {@link ByteBufOutputStream}<br>
 *
 */
public class MemoryRecordsHelper {

    private MemoryRecordsHelper() {
    }

    public static MemoryRecordsBuilder builder(
            ByteBufferOutputStream stream,
            Compression compression,
            TimestampType timestampType,
            long baseOffset
    ) {
        return builder(stream, RecordBatch.CURRENT_MAGIC_VALUE, compression, timestampType, baseOffset);
    }

    private static MemoryRecordsBuilder builder(
            ByteBufferOutputStream stream,
            byte magic,
            Compression compression,
            TimestampType timestampType,
            long baseOffset
    ) {
        long logAppendTime = RecordBatch.NO_TIMESTAMP;
        if (timestampType == TimestampType.LOG_APPEND_TIME) {
            logAppendTime = System.currentTimeMillis();
        }
        return builder(
                stream,
                magic,
                compression,
                timestampType,
                baseOffset,
                logAppendTime,
                RecordBatch.NO_PRODUCER_ID,
                RecordBatch.NO_PRODUCER_EPOCH,
                RecordBatch.NO_SEQUENCE,
                false,
                RecordBatch.NO_PARTITION_LEADER_EPOCH
        );
    }

    private static MemoryRecordsBuilder builder(
            ByteBufferOutputStream stream,
            byte magic,
            Compression compression,
            TimestampType timestampType,
            long baseOffset,
            long logAppendTime,
            long producerId,
            short producerEpoch,
            int baseSequence,
            boolean isTransactional,
            int partitionLeaderEpoch
    ) {
        return builder(
                stream,
                magic,
                compression,
                timestampType,
                baseOffset,
                logAppendTime,
                producerId,
                producerEpoch,
                baseSequence,
                isTransactional,
                false,
                partitionLeaderEpoch
        );
    }

    private static MemoryRecordsBuilder builder(
            ByteBufferOutputStream stream,
            byte magic,
            Compression compression,
            TimestampType timestampType,
            long baseOffset,
            long logAppendTime,
            long producerId,
            short producerEpoch,
            int baseSequence,
            boolean isTransactional,
            boolean isControlBatch,
            int partitionLeaderEpoch
    ) {
        return new MemoryRecordsBuilder(
                stream,
                magic,
                compression,
                timestampType,
                baseOffset,
                logAppendTime,
                producerId,
                producerEpoch,
                baseSequence,
                isTransactional,
                isControlBatch,
                partitionLeaderEpoch,
                stream.remaining()
        );
    }
}
