/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.router.topic;

import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.common.Uuid;

import io.kroxylicious.proxy.tag.ThreadSafe;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Shared topic ID cache backed by a {@link ConcurrentHashMap}.
 * One instance per router factory, shared across all connections.
 */
@ThreadSafe
class SharedTopicIdCache implements TopicIdCache {

    private final ConcurrentHashMap<Uuid, String> cache = new ConcurrentHashMap<>();

    @Override
    @Nullable
    public String resolve(Uuid topicId) {
        return cache.get(topicId);
    }

    @Override
    public void put(Uuid topicId, String topicName) {
        cache.put(topicId, topicName);
    }
}
