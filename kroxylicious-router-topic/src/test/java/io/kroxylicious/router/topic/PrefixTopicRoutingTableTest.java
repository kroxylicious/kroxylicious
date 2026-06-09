/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.router.topic;

import java.util.Map;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PrefixTopicRoutingTableTest {

    @Test
    void shouldMatchTopicByPrefix() {
        var table = PrefixTopicRoutingTable.create(
                Map.of("orders.", "cluster-a", "logs.", "cluster-b"), null);

        assertThat(table.routeForTopic("orders.uk")).isEqualTo("cluster-a");
        assertThat(table.routeForTopic("orders.us.east")).isEqualTo("cluster-a");
        assertThat(table.routeForTopic("logs.app")).isEqualTo("cluster-b");
    }

    @Test
    void shouldReturnNullWhenNoMatchAndNoDefault() {
        var table = PrefixTopicRoutingTable.create(
                Map.of("orders.", "cluster-a"), null);

        assertThat(table.routeForTopic("analytics.events")).isNull();
    }

    @Test
    void shouldReturnDefaultRouteWhenNoMatch() {
        var table = PrefixTopicRoutingTable.create(
                Map.of("orders.", "cluster-a"), "fallback");

        assertThat(table.routeForTopic("analytics.events")).isEqualTo("fallback");
    }

    @Test
    void shouldMatchExactPrefixAsTopicName() {
        var table = PrefixTopicRoutingTable.create(
                Map.of("orders", "cluster-a"), null);

        assertThat(table.routeForTopic("orders")).isEqualTo("cluster-a");
    }

    @Test
    void shouldReturnAllRoutesIncludingDefault() {
        var table = PrefixTopicRoutingTable.create(
                Map.of("orders.", "cluster-a", "logs.", "cluster-b"), "fallback");

        assertThat(table.allRoutes()).containsExactlyInAnyOrder(
                "cluster-a", "cluster-b", "fallback");
    }

    @Test
    void shouldReturnAllRoutesWithoutDefault() {
        var table = PrefixTopicRoutingTable.create(
                Map.of("orders.", "cluster-a", "logs.", "cluster-b"), null);

        assertThat(table.allRoutes()).containsExactlyInAnyOrder("cluster-a", "cluster-b");
    }

    @Test
    void shouldAllowSamePrefixOnSameRoute() {
        var table = PrefixTopicRoutingTable.create(
                Map.of("orders.", "cluster-a", "orders.uk.", "cluster-a"), null);

        assertThat(table.routeForTopic("orders.uk.london")).isEqualTo("cluster-a");
        assertThat(table.routeForTopic("orders.de")).isEqualTo("cluster-a");
    }

    @Test
    void shouldRejectOverlappingPrefixesOnDifferentRoutes() {
        assertThatThrownBy(() -> PrefixTopicRoutingTable.create(
                Map.of("orders.", "cluster-a", "orders.uk.", "cluster-b"), null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("orders.")
                .hasMessageContaining("orders.uk.")
                .hasMessageContaining("disjoint");
    }

    @Test
    void shouldMatchExplicitTopicBeforePrefix() {
        var table = PrefixTopicRoutingTable.create(
                Map.of("orders.", "cluster-a"),
                Map.of("orders.special", "cluster-b"),
                null);

        assertThat(table.routeForTopic("orders.special")).isEqualTo("cluster-b");
        assertThat(table.routeForTopic("orders.other")).isEqualTo("cluster-a");
    }

    @Test
    void shouldIncludeExplicitTopicRoutesInAllRoutes() {
        var table = PrefixTopicRoutingTable.create(
                Map.of("orders.", "cluster-a"),
                Map.of("special-topic", "cluster-b"),
                null);

        assertThat(table.allRoutes()).containsExactlyInAnyOrder("cluster-a", "cluster-b");
    }

    @Test
    void shouldRouteExplicitTopicWithNoPrefixes() {
        var table = PrefixTopicRoutingTable.create(
                Map.of(),
                Map.of("special-topic", "cluster-b"),
                "fallback");

        assertThat(table.routeForTopic("special-topic")).isEqualTo("cluster-b");
        assertThat(table.routeForTopic("other")).isEqualTo("fallback");
    }

    @Test
    void shouldRejectEmptyPrefixesWithNoDefault() {
        assertThatThrownBy(() -> PrefixTopicRoutingTable.create(Map.of(), null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("At least one prefix");
    }

    @Test
    void shouldAllowEmptyPrefixesWithDefault() {
        var table = PrefixTopicRoutingTable.create(Map.of(), "fallback");

        assertThat(table.routeForTopic("anything")).isEqualTo("fallback");
        assertThat(table.allRoutes()).containsExactly("fallback");
    }

    @Test
    void shouldNotMatchPartialPrefix() {
        var table = PrefixTopicRoutingTable.create(
                Map.of("orders.", "cluster-a"), null);

        assertThat(table.routeForTopic("order")).isNull();
        assertThat(table.routeForTopic("ordersx")).isNull();
    }

    @Test
    void shouldHandleManyPrefixes() {
        var table = PrefixTopicRoutingTable.create(Map.of(
                "a.", "r1",
                "b.", "r2",
                "c.", "r1",
                "d.", "r2",
                "e.", "r1"), null);

        assertThat(table.routeForTopic("a.topic")).isEqualTo("r1");
        assertThat(table.routeForTopic("b.topic")).isEqualTo("r2");
        assertThat(table.routeForTopic("c.topic")).isEqualTo("r1");
        assertThat(table.routeForTopic("d.topic")).isEqualTo("r2");
        assertThat(table.routeForTopic("e.topic")).isEqualTo("r1");
        assertThat(table.routeForTopic("f.topic")).isNull();
    }
}
