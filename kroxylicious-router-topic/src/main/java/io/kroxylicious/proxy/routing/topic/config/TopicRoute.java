/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.routing.topic.config;

import java.util.List;

/**
 * Associates a route name with topic name prefixes owned by that route.
 *
 * @param route the route name
 * @param topicPrefixes topic name prefixes assigned to this route
 */
public record TopicRoute(String route,
                         List<String> topicPrefixes) {}
