/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.routing.topic.config;

import java.time.Duration;
import java.util.List;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Configuration for the topic-based router.
 *
 * @param defaultRoute route for topics matching no prefix, or null if unmatched topics should be rejected
 * @param topicRoutes per-route topic prefix assignments
 * @param producerIdTtl time after last use before an idempotent producer's per-route ID mapping is evicted,
 *                      or null to use the default (7 days)
 */
public record TopicPartitionRouterConfig(@Nullable String defaultRoute,
                                         List<TopicRoute> topicRoutes,
                                         @Nullable Duration producerIdTtl) {

    public TopicPartitionRouterConfig(@Nullable String defaultRoute,
                                      List<TopicRoute> topicRoutes) {
        this(defaultRoute, topicRoutes, null);
    }
}
