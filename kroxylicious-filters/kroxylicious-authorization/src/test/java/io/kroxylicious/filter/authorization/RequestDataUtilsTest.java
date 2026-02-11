/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.authorization;

import java.nio.ByteBuffer;
import java.util.stream.Stream;

import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import edu.umd.cs.findbugs.annotations.NonNull;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.argumentSet;

public class RequestDataUtilsTest {

    public static Stream<Arguments> hasTransactionalRecords() {
        MemoryRecords transactionalRecords = memoryRecords(true);
        MemoryRecords nonTransactionalRecords = memoryRecords(false);
        return Stream.of(argumentSet("empty", new ProduceRequestData(), false),
                argumentSet("transactional", createProduceRequestData(transactionalRecords), true),
                argumentSet("non transactional", createProduceRequestData(nonTransactionalRecords), false));
    }

    @MethodSource
    @ParameterizedTest
    void hasTransactionalRecords(ProduceRequestData data, boolean expectedHasTransactionalRecords) {
        boolean hasTransactionalRecords = RequestDataUtils.hasTransactionalRecords(data);
        assertThat(hasTransactionalRecords).isEqualTo(expectedHasTransactionalRecords);
    }

    private static MemoryRecords memoryRecords(boolean transactional) {
        try (MemoryRecordsBuilder builder = new MemoryRecordsBuilder(ByteBuffer.allocate(1000), RecordBatch.CURRENT_MAGIC_VALUE, Compression.NONE,
                TimestampType.CREATE_TIME, 0L, 1L, 1L, (short) 1, 1, transactional, false, 1, 1)) {
            builder.append(1L, new byte[0], new byte[0]);
            return builder.build();
        }
    }

    @NonNull
    private static ProduceRequestData createProduceRequestData(MemoryRecords transactionalRecords) {
        ProduceRequestData.TopicProduceData topicProduceData = new ProduceRequestData.TopicProduceData();
        ProduceRequestData.PartitionProduceData partitionProduceData = new ProduceRequestData.PartitionProduceData();
        partitionProduceData.setRecords(transactionalRecords);
        topicProduceData.partitionData().add(partitionProduceData);
        ProduceRequestData produceRequestData = new ProduceRequestData();
        produceRequestData.topicData().add(topicProduceData);
        return produceRequestData;
    }

}
