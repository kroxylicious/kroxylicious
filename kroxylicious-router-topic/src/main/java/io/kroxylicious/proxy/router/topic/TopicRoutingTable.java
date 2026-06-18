/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.router.topic;

import java.util.Set;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Maps topic names to route names for topic-based router.
 */
public interface TopicRoutingTable {

    /**
     * Returns the route name that owns the given topic, or {@code null}
     * if the topic does not match any configured route.
     *
     * @param topicName the Kafka topic name
     * @return route name, or {@code null} if unroutable
     */
    @Nullable
    String routeForTopic(String topicName);

    /**
     * Returns whether the given topic name is routable (has a matching route).
     *
     * @param topicName the Kafka topic name (may be null or empty)
     * @return true if the topic has a route
     */
    default boolean isRoutable(String topicName) {
        return topicName != null && !topicName.isEmpty() && routeForTopic(topicName) != null;
    }

    /**
     * Returns the set of all route names known to this table.
     */
    Set<String> allRoutes();
}
