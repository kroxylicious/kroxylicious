/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter;

import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.common.Uuid;

/**
 * The result of discovering the topic names for a collection of topic ids
 * @param topicNameResults
 */
public record TopicNameMapping(Map<Uuid, TopicNameResult> topicNameResults) {

    /**
     * @return a map from topic id to topic name lookup exception for all failed lookups
     */
    Map<Uuid, TopicNameLookupException> failedResults() {
        return topicNameResults.entrySet().stream().filter(e -> e.getValue().exception() != null)
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().exception()));
    }

    /**
     * @return a map of all successfully discovered topic names
     */
    Map<Uuid, String> successfulResults() {
        return topicNameResults.entrySet().stream().filter(e -> e.getValue().topicName() != null)
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().topicName()));
    }

    /**
     * @return true if any of the results were failures
     */
    boolean anyFailedResults() {
        return topicNameResults.values().stream().anyMatch(topicNameResult -> topicNameResult.exception() != null);
    }
}
