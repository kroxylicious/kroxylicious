/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.validation.validators.topic;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.record.BaseRecords;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Record;

import io.kroxylicious.proxy.filter.validation.validators.record.RecordValidator;

class PerRecordTopicValidator implements TopicValidator {

    private final RecordValidator validator;

    PerRecordTopicValidator(RecordValidator validator) {
        if (validator == null) {
            throw new IllegalArgumentException("validator was null");
        }
        this.validator = validator;
    }

    @Override
    public CompletionStage<TopicValidationResult> validateTopicData(ProduceRequestData.TopicProduceData topicProduceData) {
        CompletableFuture<PartitionValidationResult>[] result = topicProduceData.partitionData()
                                                                                .stream()
                                                                                .map(this::validateTopicPartition)
                                                                                .map(CompletionStage::toCompletableFuture)
                                                                                .toArray(CompletableFuture[]::new);
        return CompletableFuture.allOf(result).thenApply(unused -> {
            Map<Integer, PartitionValidationResult> collect = Arrays.stream(result)
                                                                    .map(CompletableFuture::join)
                                                                    .collect(Collectors.toMap(PartitionValidationResult::index, x -> x));
            return new PerPartitionTopicValidationResult(topicProduceData.name(), collect);
        });
    }

    private CompletionStage<PartitionValidationResult> validateTopicPartition(ProduceRequestData.PartitionProduceData partitionProduceData) {
        BaseRecords records = partitionProduceData.records();
        if (!(records instanceof MemoryRecords)) {
            return CompletableFuture.completedFuture(new PartitionValidationResult(partitionProduceData.index(), List.of()));
        }
        int recordIndex = 0;
        CompletableFuture<List<RecordValidationFailure>> result = CompletableFuture.completedFuture(new ArrayList<>());
        for (Record record : ((MemoryRecords) records).records()) {
            int finalRecordIndex = recordIndex;
            result = result.thenCompose(recordValidationFailures -> validator.validate(record).thenApply(result1 -> {
                if (!result1.valid()) {
                    recordValidationFailures.add(new RecordValidationFailure(finalRecordIndex, result1.errorMessage()));
                }
                return recordValidationFailures;
            }));
            recordIndex++;
        }
        return result.thenApply(recordValidationFailures -> new PartitionValidationResult(partitionProduceData.index(), recordValidationFailures));
    }
}
