/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.FetchResponseData.FetchableTopicResponse;
import org.apache.kafka.common.message.FetchResponseData.PartitionData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceRequestData.TopicProduceData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.utils.ByteBufferOutputStream;

import io.kroxylicious.proxy.filter.FetchResponseFilter;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.ProduceRequestFilter;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.filter.ResponseFilterResult;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * A filter for encrypting and decrypting records using envelope encryption
 * @param <K> The type of KEK reference
 */
public class EnvelopeEncryptionFilter<K>
        implements ProduceRequestFilter, FetchResponseFilter {
    private final TopicNameBasedKekSelector<K> kekSelector;

    private final KeyManager<K> keyManager;

    EnvelopeEncryptionFilter(KeyManager<K> keyManager, TopicNameBasedKekSelector<K> kekSelector) {
        this.kekSelector = kekSelector;
        this.keyManager = keyManager;
    }

    @SuppressWarnings("unchecked")
    public static <T> CompletionStage<List<T>> join(List<? extends CompletionStage<T>> stages) {
        CompletableFuture<T>[] futures = new CompletableFuture[stages.size()];
        for (int i = 0; i < stages.size(); i++) {
            futures[i] = stages.get(i).toCompletableFuture();
        }
        return CompletableFuture.allOf(futures)
                .thenApply(ignored -> Stream.of(futures).map(CompletableFuture::join).toList());
    }

    @Override
    public CompletionStage<RequestFilterResult> onProduceRequest(short apiVersion,
                                                                 RequestHeaderData header,
                                                                 ProduceRequestData request,
                                                                 FilterContext context) {
        return maybeEncodeProduce(request, context)
                .thenCompose(yy -> context.forwardRequest(header, request));
    }

    private CompletionStage<ProduceRequestData> maybeEncodeProduce(ProduceRequestData request, FilterContext context) {
        var topicNameToData = request.topicData().stream().collect(Collectors.toMap(TopicProduceData::name, Function.identity()));
        return kekSelector.selectKek(topicNameToData.keySet()) // figure out what keks we need
                .thenCompose(kekMap -> {
                    var futures = kekMap.entrySet().stream().flatMap(e -> {
                        String topicName = e.getKey();
                        var kekId = e.getValue();
                        TopicProduceData tpd = topicNameToData.get(topicName);
                        return tpd.partitionData().stream().map(ppd -> {
                            // handle case where this topic is to be left unencrypted
                            if (kekId == null) {
                                return CompletableFuture.completedStage(ppd);
                            }
                            MemoryRecords records = (MemoryRecords) ppd.records();
                            MemoryRecordsBuilder builder = recordsBuilder(allocateBufferForEncode(records, context), records);
                            var encryptionRequests = recordStream(records).toList();
                            return keyManager.encrypt(
                                    new EncryptionScheme<>(kekId, EnumSet.of(RecordField.RECORD_VALUE)),
                                    encryptionRequests,
                                    (kafkaRecord, encryptedValue, headers) -> builder.append(kafkaRecord.timestamp(), kafkaRecord.key(), encryptedValue, headers))
                                    .thenApply(ignored -> ppd.setRecords(builder.build()));
                        });
                    }).toList();
                    return join(futures).thenApply(x -> request);
                });
    }

    private ByteBufferOutputStream allocateBufferForEncode(MemoryRecords records, FilterContext context) {
        int sizeEstimate = 2 * records.sizeInBytes();
        // Accurate estimation is tricky without knowing the record sizes
        return context.createByteBufferOutputStream(sizeEstimate);
    }

    @Override
    public CompletionStage<ResponseFilterResult> onFetchResponse(short apiVersion, ResponseHeaderData header, FetchResponseData response, FilterContext context) {
        return maybeDecodeFetch(response.responses(), context)
                .thenCompose(responses -> context.forwardResponse(header, response.setResponses(responses)));
    }

    private CompletionStage<List<FetchableTopicResponse>> maybeDecodeFetch(List<FetchableTopicResponse> topics, FilterContext context) {
        List<CompletionStage<FetchableTopicResponse>> result = new ArrayList<>(topics.size());
        for (FetchableTopicResponse topicData : topics) {
            result.add(maybeDecodePartitions(topicData.partitions(), context).thenApply(kk -> {
                topicData.setPartitions(kk);
                return topicData;
            }));
        }
        return join(result);
    }

    private CompletionStage<List<PartitionData>> maybeDecodePartitions(List<PartitionData> partitions, FilterContext context) {
        List<CompletionStage<PartitionData>> result = new ArrayList<>(partitions.size());
        for (PartitionData partitionData : partitions) {
            if (!(partitionData.records() instanceof MemoryRecords)) {
                throw new IllegalStateException();
            }
            result.add(maybeDecodeRecords(partitionData, (MemoryRecords) partitionData.records(), context));
        }
        return join(result);
    }

    private CompletionStage<PartitionData> maybeDecodeRecords(PartitionData fpr,
                                                              MemoryRecords memoryRecords,
                                                              FilterContext context) {
        MemoryRecordsBuilder builder = recordsBuilder(allocateBufferForDecode(memoryRecords, context), memoryRecords);
        return keyManager.decrypt(recordStream(memoryRecords).toList(),
                (kafkaRecord, plaintextBuffer, headers) -> builder.append(kafkaRecord.timestamp(), kafkaRecord.key(), plaintextBuffer, headers))
                .thenApply(ignored -> builder.build())
                .thenApply(fpr::setRecords);
    }

    private ByteBufferOutputStream allocateBufferForDecode(MemoryRecords memoryRecords, FilterContext context) {
        int sizeEstimate = memoryRecords.sizeInBytes();
        return context.createByteBufferOutputStream(sizeEstimate);
    }

    @NonNull
    private static Stream<org.apache.kafka.common.record.Record> recordStream(MemoryRecords memoryRecords) {
        return StreamSupport.stream(memoryRecords.records().spliterator(), false);
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
