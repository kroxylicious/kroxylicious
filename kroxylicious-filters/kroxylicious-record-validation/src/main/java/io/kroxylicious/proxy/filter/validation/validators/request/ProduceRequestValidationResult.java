/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.validation.validators.request;

import java.util.Map;
import java.util.stream.Stream;

import io.kroxylicious.proxy.filter.validation.validators.topic.PartitionValidationResult;
import io.kroxylicious.proxy.filter.validation.validators.topic.TopicValidationResult;

/**
 * The result of validating an entire {@link org.apache.kafka.common.message.ProduceRequestData}. Contains
 * validation results for each topic in the request.
 * @param topicValidationResults results per topic, key is topicName
 */
public record ProduceRequestValidationResult(Map<String, TopicValidationResult> topicValidationResults) {

    /**
     * Is any topic partition invalid
     * @return true if any topic-partitions were invalid
     */
    public boolean isAnyTopicPartitionInvalid() {
        return topicValidationResults.values().stream().anyMatch(TopicValidationResult::isAnyPartitionInvalid);
    }

    /**
     * Are all topic partitions invalid
     * @return true if all topic-partitions were invalid
     */
    public boolean isAllTopicPartitionsInvalid() {
        return topicValidationResults.values().stream().allMatch(TopicValidationResult::isAllPartitionsInvalid);
    }

    /**
     * Are all topic partitions invalid for a topic
     * @param topicName topic name
     * @return true if all partitions for topicName are invalid
     */
    public boolean isAllPartitionsInvalid(String topicName) {
        TopicValidationResult topicValidationResult = topicValidationResults.get(topicName);
        if (topicValidationResult == null) {
            return false;
        } else {
            return topicValidationResult.isAllPartitionsInvalid();
        }
    }

    /**
     * Is a topic-partition valid
     * @param topicName name of the topic
     * @param partitionIndex index of the partition
     * @return true if the topic-partition is valid
     * @throws IllegalStateException if the topicName or partitionIndex has no recorded result, we should have some outcome for every topic-partition
     */
    public boolean isPartitionValid(String topicName, int partitionIndex) {
        TopicValidationResult topicValidationResult = topicValidationResults.get(topicName);
        if (topicValidationResult == null) {
            throw new IllegalStateException("topicValidationResults should contain a result for all topics in the request, failed topic: " + topicName);
        }
        PartitionValidationResult partitionValidationResult = topicValidationResult.getPartitionResult(partitionIndex);
        if (partitionValidationResult == null) {
            throw new IllegalStateException(
                    "partitionValidationResult should contain a result for all topics in the request, failed topic: "
                                            + topicName
                                            + ", partition"
                                            + partitionIndex
            );
        }
        return partitionValidationResult.allRecordsValid();
    }

    /**
     * Get all topics with invalid partitions
     * @return stream of topic results containing invalid partitions
     */
    public Stream<TopicValidationResult> topicsWithInvalidPartitions() {
        return topicValidationResults.values().stream().filter(TopicValidationResult::isAnyPartitionInvalid);
    }

    /**
     * Get topic result for a topic
     * @param topicName name of topic
     * @return result
     */
    public TopicValidationResult topicResult(String topicName) {
        return topicValidationResults.get(topicName);
    }
}
