/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.validation.validators.topic;

import java.util.Map;
import java.util.stream.Stream;

record PerPartitionTopicValidationResult(String topicName, Map<Integer, PartitionValidationResult> partitionValidationResults) implements TopicValidationResult {

    @Override
    public boolean isAnyPartitionInvalid() {
        return partitionValidationResults.values().stream().anyMatch(partitionValidationResult -> !partitionValidationResult.allRecordsValid());
    }

    @Override
    public Stream<PartitionValidationResult> invalidPartitions() {
        return partitionValidationResults.values().stream().filter(partitionValidationResult -> !partitionValidationResult.allRecordsValid());
    }

    @Override
    public PartitionValidationResult getPartitionResult(int index) {
        return partitionValidationResults.get(index);
    }
}
