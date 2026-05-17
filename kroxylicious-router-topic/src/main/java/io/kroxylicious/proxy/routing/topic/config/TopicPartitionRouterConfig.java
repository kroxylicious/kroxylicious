/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.routing.topic.config;

import java.util.List;

/**
 * Configuration for the topic-based router.
 *
 * @param defaultRoute route for topics matching no prefix, or null if unmatched topics should be rejected
 * @param topicRoutes per-route topic prefix assignments
 */
public record TopicPartitionRouterConfig(String defaultRoute,
                                         List<TopicRoute> topicRoutes) {}
