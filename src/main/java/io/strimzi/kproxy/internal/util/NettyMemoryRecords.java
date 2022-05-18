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
package io.strimzi.kproxy.internal.util;

import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;

import io.netty.buffer.ByteBuf;

/**
 * This is introduces additional factory builder methods then {@link org.apache.kafka.common.record.MemoryRecords#builder} ones,
 * in order to use {@link ByteBufOutputStream}<br>
 *
 */
public class NettyMemoryRecords {

    public static MemoryRecordsBuilder builder(ByteBuf buffer,
                                               CompressionType compressionType,
                                               TimestampType timestampType,
                                               long baseOffset) {
        return builder(new ByteBufOutputStream(buffer), RecordBatch.CURRENT_MAGIC_VALUE, compressionType, timestampType, baseOffset);
    }

    private static MemoryRecordsBuilder builder(ByteBufOutputStream stream,
                                                CompressionType compressionType,
                                                TimestampType timestampType,
                                                long baseOffset) {
        return builder(stream, RecordBatch.CURRENT_MAGIC_VALUE, compressionType, timestampType, baseOffset);
    }

    private static MemoryRecordsBuilder builder(ByteBufOutputStream stream,
                                                byte magic,
                                                CompressionType compressionType,
                                                TimestampType timestampType,
                                                long baseOffset) {
        long logAppendTime = RecordBatch.NO_TIMESTAMP;
        if (timestampType == TimestampType.LOG_APPEND_TIME)
            logAppendTime = System.currentTimeMillis();
        return builder(stream, magic, compressionType, timestampType, baseOffset, logAppendTime,
                RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_SEQUENCE, false,
                RecordBatch.NO_PARTITION_LEADER_EPOCH);
    }

    private static MemoryRecordsBuilder builder(ByteBufOutputStream stream,
                                                byte magic,
                                                CompressionType compressionType,
                                                TimestampType timestampType,
                                                long baseOffset,
                                                long logAppendTime,
                                                long producerId,
                                                short producerEpoch,
                                                int baseSequence,
                                                boolean isTransactional,
                                                int partitionLeaderEpoch) {
        return builder(stream, magic, compressionType, timestampType, baseOffset,
                logAppendTime, producerId, producerEpoch, baseSequence, isTransactional, false, partitionLeaderEpoch);
    }

    private static MemoryRecordsBuilder builder(ByteBufOutputStream stream,
                                                byte magic,
                                                CompressionType compressionType,
                                                TimestampType timestampType,
                                                long baseOffset,
                                                long logAppendTime,
                                                long producerId,
                                                short producerEpoch,
                                                int baseSequence,
                                                boolean isTransactional,
                                                boolean isControlBatch,
                                                int partitionLeaderEpoch) {
        return new MemoryRecordsBuilder(stream, magic, compressionType, timestampType, baseOffset,
                logAppendTime, producerId, producerEpoch, baseSequence, isTransactional, isControlBatch, partitionLeaderEpoch,
                stream.remaining());
    }
}
