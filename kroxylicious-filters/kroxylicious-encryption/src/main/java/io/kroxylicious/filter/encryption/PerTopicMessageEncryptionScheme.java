/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.IntFunction;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.utils.ByteBufferOutputStream;

import edu.umd.cs.findbugs.annotations.NonNull;

public class PerTopicMessageEncryptionScheme<K> implements MessageEncryptionScheme<K> {

    private final Map<String, RecordEncryptionScheme<K>> encryptionSchemePerTopic;
    private final RecordEncryptionScheme<K> defaultRecordEncryptionScheme;

    public PerTopicMessageEncryptionScheme(Map<String, RecordEncryptionScheme<K>> encryptionSchemePerTopic, RecordEncryptionScheme<K> defaultRecordEncryptionScheme) {
        this.encryptionSchemePerTopic = encryptionSchemePerTopic;
        this.defaultRecordEncryptionScheme = defaultRecordEncryptionScheme;
    }

    @Override
    public CompletionStage<ProduceRequestData> encrypt(RequestHeaderData recordHeaders, ProduceRequestData requestData,
                                                       IntFunction<ByteBufferOutputStream> bufferFactory) {
        final ProduceRequestData.TopicProduceDataCollection topicProduceData = requestData.topicData();
        List<CompletionStage<ProduceRequestData.PartitionProduceData>> encryptedPartitionDataStages = new ArrayList<>();
        for (ProduceRequestData.TopicProduceData produceData : topicProduceData) {
            for (ProduceRequestData.PartitionProduceData partitionProduceData : produceData.partitionData()) {
                final String topic = produceData.name();

                MemoryRecords records = (MemoryRecords) partitionProduceData.records();
                MemoryRecordsBuilder builder = recordsBuilder(allocateBufferForEncode(records, bufferFactory), records);
                encryptedPartitionDataStages.add(EnvelopeEncryptionFilter.join(recordStream(records)
                        .map(kafkaRecord -> encryptionSchemePerTopic.getOrDefault(topic, defaultRecordEncryptionScheme)
                                .encrypt(kafkaRecord, recordHeaders, requestData)
                                .thenCompose(encryptedRecord -> {
                                    builder.append(encryptedRecord);
                                    return CompletableFuture.completedStage(encryptedRecord);
                                }))
                        .toList())
                        .thenCompose(voids -> {
                            partitionProduceData.setRecords(builder.build());
                            return CompletableFuture.completedStage(partitionProduceData);
                        }));
            }
        }

        return EnvelopeEncryptionFilter.join(encryptedPartitionDataStages).thenCompose(ignored -> CompletableFuture.completedStage(requestData));
    }

    @Override
    public CompletionStage<FetchResponseData> decrypt(ResponseHeaderData header, FetchResponseData response) {
        return null;
    }

    @NonNull
    private static Stream<Record> recordStream(MemoryRecords memoryRecords) {
        return StreamSupport.stream(memoryRecords.records().spliterator(), false);
    }

    private ByteBufferOutputStream allocateBufferForEncode(MemoryRecords records, IntFunction<ByteBufferOutputStream> bufferFactory) {
        int sizeEstimate = 2 * records.sizeInBytes();
        // Accurate estimation is tricky without knowing the record sizes
        return bufferFactory.apply(sizeEstimate);
    }

    private static MemoryRecordsBuilder recordsBuilder(@NonNull ByteBufferOutputStream buffer, @NonNull MemoryRecords records) {
        RecordBatch firstBatch = records.firstBatch();
        return new MemoryRecordsBuilder(buffer,
                firstBatch.magic(),
                firstBatch.compressionType(), // TODO we might not want to use the client's compression
                firstBatch.timestampType(),
                firstBatch.baseOffset(),
                0L,
                firstBatch.producerId(),
                firstBatch.producerEpoch(),
                firstBatch.baseSequence(),
                firstBatch.isTransactional(),
                firstBatch.isControlBatch(),
                firstBatch.partitionLeaderEpoch(),
                0);
    }
}
