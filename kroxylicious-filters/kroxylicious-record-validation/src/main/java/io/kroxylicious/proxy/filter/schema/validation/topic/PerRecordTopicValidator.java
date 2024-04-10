/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.schema.validation.topic;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.record.BaseRecords;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.record.Record;

import io.kroxylicious.proxy.filter.schema.validation.Result;
import io.kroxylicious.proxy.filter.schema.validation.record.RecordValidator;

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
        PerPartitionTopicValidationResult result = new PerPartitionTopicValidationResult(topicProduceData.name(),
                topicProduceData.partitionData().stream().collect(Collectors.toMap(
                        ProduceRequestData.PartitionProduceData::index, this::validateTopicPartition)));
        return CompletableFuture.completedFuture(result);
    }

    private PartitionValidationResult validateTopicPartition(ProduceRequestData.PartitionProduceData partitionProduceData) {
        return new PartitionValidationResult(partitionProduceData.index(), validateRecords(partitionProduceData.records()));
    }

    private List<RecordValidationFailure> validateRecords(BaseRecords records) {
        if (!(records instanceof MemoryRecords)) {
            return List.of();
        }
        int recordIndex = 0;
        List<RecordValidationFailure> failures = new ArrayList<>();
        for (MutableRecordBatch batch : ((MemoryRecords) records).batches()) {
            for (Record record : batch) {
                // todo make partition validation async to avoid joining
                CompletableFuture<Result> result = validator.validate(record).toCompletableFuture();
                if (!result.join().valid()) {
                    failures.add(new RecordValidationFailure(recordIndex, result.join().errorMessage()));
                }
                recordIndex++;
            }
        }
        return failures;
    }
}
