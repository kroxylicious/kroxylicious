/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.validation.validators.request;

import java.util.Map;
import java.util.stream.Stream;

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