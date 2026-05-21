/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.routing.topic.config;

import java.util.List;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Configuration for a single route, defining the topics and subjects it owns.
 *
 * @param name the route name (must match a route defined in the router's {@code routes} section)
 * @param topicPrefixes topic name prefixes assigned to this route, or null if none
 * @param topics explicit topic names assigned to this route, or null if none
 * @param subjects authenticated usernames that are subject-routed to this route, or null if none.
 *                 Subject-routed users have all their coordinator-bound and data-plane operations
 *                 locked to this route (except METADATA and admin operations which still fan out).
 */
public record RouteConfig(String name,
                          @Nullable List<String> topicPrefixes,
                          @Nullable List<String> topics,
                          @Nullable List<String> subjects) {

    public RouteConfig(String name,
                       @Nullable List<String> topicPrefixes) {
        this(name, topicPrefixes, null, null);
    }
}
