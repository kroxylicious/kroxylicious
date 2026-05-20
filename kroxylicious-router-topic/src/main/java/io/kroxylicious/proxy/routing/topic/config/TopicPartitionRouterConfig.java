/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.routing.topic.config;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Configuration for the topic-based router.
 *
 * @param defaultRoute route for topics matching no prefix, or null if unmatched topics should be rejected
 * @param topicRoutes per-route topic prefix assignments
 * @param transactionalUserRoutes mapping from authenticated username to route name for transaction routing,
 *                                or null if all transactions should use the default route
 * @param consumerGroupUserRoutes mapping from authenticated username to route name for consumer group
 *                                coordinator routing, or null if all consumer groups should use the default route
 * @param producerIdTtl time after last use before an idempotent producer's per-route ID mapping is evicted,
 *                      or null to use the default (7 days)
 * @param maxFetchSessionCacheSlots maximum number of concurrent client-side fetch sessions across all connections,
 *                                  or null to use the default (1000)
 * @param minFetchSessionEviction minimum idle time before a fetch session can be evicted from the cache,
 *                                or null to use the default (120 seconds)
 */
public record TopicPartitionRouterConfig(@Nullable String defaultRoute,
                                         List<TopicRoute> topicRoutes,
                                         @Nullable Map<String, String> transactionalUserRoutes,
                                         @Nullable Map<String, String> consumerGroupUserRoutes,
                                         @Nullable Duration producerIdTtl,
                                         @Nullable Integer maxFetchSessionCacheSlots,
                                         @Nullable Duration minFetchSessionEviction) {

    public TopicPartitionRouterConfig(@Nullable String defaultRoute,
                                      List<TopicRoute> topicRoutes) {
        this(defaultRoute, topicRoutes, null, null, null, null, null);
    }

    public TopicPartitionRouterConfig(@Nullable String defaultRoute,
                                      List<TopicRoute> topicRoutes,
                                      @Nullable Duration producerIdTtl) {
        this(defaultRoute, topicRoutes, null, null, producerIdTtl, null, null);
    }

    public TopicPartitionRouterConfig(@Nullable String defaultRoute,
                                      List<TopicRoute> topicRoutes,
                                      @Nullable Duration producerIdTtl,
                                      @Nullable Integer maxFetchSessionCacheSlots,
                                      @Nullable Duration minFetchSessionEviction) {
        this(defaultRoute, topicRoutes, null, null, producerIdTtl, maxFetchSessionCacheSlots, minFetchSessionEviction);
    }

    public TopicPartitionRouterConfig(@Nullable String defaultRoute,
                                      List<TopicRoute> topicRoutes,
                                      @Nullable Map<String, String> transactionalUserRoutes,
                                      @Nullable Duration producerIdTtl,
                                      @Nullable Integer maxFetchSessionCacheSlots,
                                      @Nullable Duration minFetchSessionEviction) {
        this(defaultRoute, topicRoutes, transactionalUserRoutes, null,
                producerIdTtl, maxFetchSessionCacheSlots, minFetchSessionEviction);
    }
}
