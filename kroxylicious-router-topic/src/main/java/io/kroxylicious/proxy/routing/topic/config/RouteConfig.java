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
 * @param transactionalUsers authenticated usernames whose transactions route here, or null if none
 * @param consumerGroupUsers authenticated usernames whose consumer groups route here, or null if none
 */
public record RouteConfig(String name,
                          @Nullable List<String> topicPrefixes,
                          @Nullable List<String> topics,
                          @Nullable List<String> transactionalUsers,
                          @Nullable List<String> consumerGroupUsers) {

    public RouteConfig(String name,
                       @Nullable List<String> topicPrefixes) {
        this(name, topicPrefixes, null, null, null);
    }
}
