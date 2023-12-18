/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
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

/**
 * Describes how a record should be encrypted
 *
 * @param <K> The type of KEK identifier.
 * @param recordFields The fields of the record that should be encrypted with the given KEK. Neither null nor empty.
 */
public record KekPerTopicEncryptionScheme<K>(
                                             KeyManager<K> keyManager,
                                             Map<String, K> keksByTopicName,
                                             Set<RecordField> recordFields)
        implements EncryptionScheme<K> {
    public KekPerTopicEncryptionScheme {
        Objects.requireNonNull(keksByTopicName);
        if (Objects.requireNonNull(recordFields).isEmpty()) {
            throw new IllegalArgumentException();
        }
    }

    @Override
    public CompletionStage<ProduceRequestData> encrypt(RequestHeaderData recordHeaders, ProduceRequestData requestData,
                                                       IntFunction<ByteBufferOutputStream> bufferFactory) {
        var topicNameToData = requestData.topicData().stream().collect(Collectors.toMap(ProduceRequestData.TopicProduceData::name, Function.identity()));
        var futures = keksByTopicName.entrySet().stream().flatMap(e -> {
            String topicName = e.getKey();
            var kekId = e.getValue();
            ProduceRequestData.TopicProduceData tpd = topicNameToData.get(topicName);
            return tpd.partitionData().stream().map(ppd -> {
                // handle case where this topic is to be left unencrypted
                if (kekId == null) {
                    return CompletableFuture.completedStage(ppd);
                }
                MemoryRecords records = (MemoryRecords) ppd.records();
                MemoryRecordsBuilder builder = recordsBuilder(allocateBufferForEncode(records, bufferFactory), records);
                var encryptionRequests = recordStream(records).toList();
                return keyManager.encrypt(
                        topicName,
                        ppd.index(),
                        new SingleKekEncryptionScheme<>(kekId, recordFields),
                        encryptionRequests,
                        (kafkaRecord, encryptedValue, headers) -> builder.append(kafkaRecord.timestamp(), kafkaRecord.key(), encryptedValue, headers))
                        .thenApply(ignored -> ppd.setRecords(builder.build()));
            });
        }).toList();

        return EnvelopeEncryptionFilter.join(futures).thenApply(x -> requestData);
    }

    @Override
    public CompletionStage<FetchResponseData> decrypt(ResponseHeaderData header, FetchResponseData response) {
        return null;
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

    @NonNull
    private static Stream<Record> recordStream(MemoryRecords memoryRecords) {
        return StreamSupport.stream(memoryRecords.records().spliterator(), false);
    }

}
