/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.util.Map;
import java.util.Objects;

import org.apache.kafka.common.Uuid;

import io.kroxylicious.proxy.filter.metadata.TopicNameMapping;
import io.kroxylicious.proxy.filter.metadata.TopicNameMappingException;

/**
 * The result of discovering the topic names for a collection of topic ids
 * @param topicNames successfully mapped topic names, non-null
 * @param failures failed topic name mappings, non-null
 */
public record MapTopicNameMapping(Map<Uuid, String> topicNames, Map<Uuid, TopicNameMappingException> failures) implements TopicNameMapping {

    public static final TopicNameMapping EMPTY = new MapTopicNameMapping(Map.of(), Map.of());

    public MapTopicNameMapping {
        Objects.requireNonNull(topicNames);
        Objects.requireNonNull(failures);
    }

    @Override
    public boolean anyFailures() {
        return !failures.isEmpty();
    }
}
