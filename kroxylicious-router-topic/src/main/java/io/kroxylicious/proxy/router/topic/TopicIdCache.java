/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.router.topic;

import org.apache.kafka.common.Uuid;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Cache mapping Kafka topic IDs to topic names. Used by the router to
 * resolve topicId-bearing requests (e.g. PRODUCE v13) to topic names
 * for routing.
 */
interface TopicIdCache {

    /**
     * Resolves a topic ID to its topic name.
     *
     * @param topicId the topic ID to resolve
     * @return the topic name, or null if not cached
     */
    @Nullable
    String resolve(Uuid topicId);

    /**
     * Caches a topic ID to topic name mapping.
     *
     * @param topicId the topic ID
     * @param topicName the topic name
     */
    void put(Uuid topicId, String topicName);
}
