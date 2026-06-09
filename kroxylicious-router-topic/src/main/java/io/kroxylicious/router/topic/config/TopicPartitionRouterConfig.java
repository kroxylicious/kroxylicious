/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.router.topic.config;

import java.time.Duration;
import java.util.List;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Configuration for the topic-based router.
 *
 * @param defaultRoute route for topics matching no prefix or explicit name, or null if unmatched topics should be rejected
 * @param routes per-route configuration defining topic ownership and subject-based router
 * @param producerIdTtl time after last use before an idempotent producer's per-route ID mapping is evicted,
 *                      or null to use the default (7 days)
 * @param maxFetchSessionCacheSlots maximum number of concurrent client-side fetch sessions across all connections,
 *                                  or null to use the default (1000)
 * @param minFetchSessionEviction minimum idle time before a fetch session can be evicted from the cache,
 *                                or null to use the default (120 seconds)
 */
public record TopicPartitionRouterConfig(@Nullable String defaultRoute,
                                         List<RouteConfig> routes,
                                         @Nullable Duration producerIdTtl,
                                         @Nullable Integer maxFetchSessionCacheSlots,
                                         @Nullable Duration minFetchSessionEviction) {

    public TopicPartitionRouterConfig(@Nullable String defaultRoute,
                                      List<RouteConfig> routes) {
        this(defaultRoute, routes, null, null, null);
    }
}
