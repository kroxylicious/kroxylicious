/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.validation.validators.topic;

import java.util.stream.Stream;

/**
 * Result of validating TopicProducerData
 */
public interface TopicValidationResult {

    /**
     * is any partition invalid
     * @return true if any partition invalid
     */
    boolean isAnyPartitionInvalid();

    /**
     * get invalid partitions
     * @return stream of invalid partitions
     */
    Stream<PartitionValidationResult> invalidPartitions();

    /**
     * name of validated topic
     * @return topicName
     */
    String topicName();

    /**
     * get partition result
     * @param index partition index
     * @return result for partition
     */
    PartitionValidationResult getPartitionResult(int index);
}
