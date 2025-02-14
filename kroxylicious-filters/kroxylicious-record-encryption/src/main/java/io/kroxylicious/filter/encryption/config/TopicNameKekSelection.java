/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.config;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Selection of keys
 * @param topicNameToKekId topic name to key, key must not be null
 * @param unresolvedTopicNames topic names which could not have a key selected
 * @param <K> the type of key
 */
public record TopicNameKekSelection<K>(@NonNull Map<String, K> topicNameToKekId, @NonNull Set<String> unresolvedTopicNames) {
    public TopicNameKekSelection {
        Objects.requireNonNull(topicNameToKekId);
        List<Map.Entry<String, K>> entriesWithNullValue = topicNameToKekId.entrySet().stream().filter(e -> e.getValue() == null).toList();
        if (!entriesWithNullValue.isEmpty()) {
            List<String> topicNames = entriesWithNullValue.stream().map(Map.Entry::getKey).toList();
            throw new IllegalArgumentException("values in topicNameToKekId must not be null, keys with null values: " + topicNames);
        }
        Objects.requireNonNull(unresolvedTopicNames);
    }
}
