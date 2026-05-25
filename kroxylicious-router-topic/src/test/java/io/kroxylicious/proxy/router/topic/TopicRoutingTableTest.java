/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.routing.topic;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class TopicRoutingTableTest {

    @Test
    void shouldReturnRouteForKnownTopic() {
        var table = mapBacked(Map.of("orders.uk", "cluster-a", "logs.app", "cluster-b"));

        assertThat(table.routeForTopic("orders.uk")).isEqualTo("cluster-a");
        assertThat(table.routeForTopic("logs.app")).isEqualTo("cluster-b");
    }

    @Test
    void shouldReturnNullForUnknownTopic() {
        var table = mapBacked(Map.of("orders.uk", "cluster-a"));

        assertThat(table.routeForTopic("unknown")).isNull();
    }

    @Test
    void shouldReturnAllRoutes() {
        var table = mapBacked(Map.of("t1", "route-a", "t2", "route-b", "t3", "route-a"));

        assertThat(table.allRoutes()).containsExactlyInAnyOrder("route-a", "route-b");
    }

    private static TopicRoutingTable mapBacked(Map<String, String> topicToRoute) {
        var routes = Set.copyOf(topicToRoute.values());
        var map = new HashMap<>(topicToRoute);
        return new TopicRoutingTable() {
            @Override
            public String routeForTopic(String topicName) {
                return map.get(topicName);
            }

            @Override
            public Set<String> allRoutes() {
                return routes;
            }
        };
    }
}
