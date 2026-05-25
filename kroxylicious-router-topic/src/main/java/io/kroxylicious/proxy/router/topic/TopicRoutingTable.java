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
     * Returns the set of all route names known to this table.
     */
    Set<String> allRoutes();
}
