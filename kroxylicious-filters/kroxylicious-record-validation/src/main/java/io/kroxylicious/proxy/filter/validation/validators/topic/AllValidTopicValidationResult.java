/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.validation.validators.topic;

import java.util.List;
import java.util.stream.Stream;

record AllValidTopicValidationResult(String topicName) implements TopicValidationResult {

    @Override
    public boolean isAnyPartitionInvalid() {
        return false;
    }

    @Override
    public Stream<PartitionValidationResult> invalidPartitions() {
        return Stream.empty();
    }

    @Override
    public PartitionValidationResult getPartitionResult(int index) {
        return new PartitionValidationResult(index, List.of());
    }
}
