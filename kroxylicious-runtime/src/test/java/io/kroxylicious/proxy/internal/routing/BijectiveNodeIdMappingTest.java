/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.routing;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.internal.routing.NodeIdMapping.RouteAndNode;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BijectiveNodeIdMappingTest {

    private static final String ROUTE_A = "route-a";
    private static final String ROUTE_B = "route-b";
    private static final String ROUTE_C = "route-c";

    @Test
    void shouldRoundTripTwoRoutes() {
        var mapping = new BijectiveNodeIdMapping(Map.of(ROUTE_A, 0, ROUTE_B, 1), 2);
        for (int t = 0; t <= 100; t++) {
            for (String route : List.of(ROUTE_A, ROUTE_B)) {
                int v = mapping.toVirtual(route, t);
                RouteAndNode result = mapping.fromVirtual(v);
                assertThat(result).isEqualTo(new RouteAndNode(route, t));
            }
        }
    }

    @Test
    void shouldRoundTripThreeRoutes() {
        var mapping = new BijectiveNodeIdMapping(Map.of(ROUTE_A, 0, ROUTE_B, 1, ROUTE_C, 2), 3);
        for (int t = 0; t <= 50; t++) {
            for (String route : List.of(ROUTE_A, ROUTE_B, ROUTE_C)) {
                int v = mapping.toVirtual(route, t);
                RouteAndNode result = mapping.fromVirtual(v);
                assertThat(result).isEqualTo(new RouteAndNode(route, t));
            }
        }
    }

    @Test
    void shouldAssignInterleavedVirtualIds() {
        var mapping = new BijectiveNodeIdMapping(Map.of(ROUTE_A, 0, ROUTE_B, 1), 2);
        assertThat(mapping.toVirtual(ROUTE_A, 0)).isZero();
        assertThat(mapping.toVirtual(ROUTE_B, 0)).isEqualTo(1);
        assertThat(mapping.toVirtual(ROUTE_A, 1)).isEqualTo(2);
        assertThat(mapping.toVirtual(ROUTE_B, 1)).isEqualTo(3);
        assertThat(mapping.toVirtual(ROUTE_A, 2)).isEqualTo(4);
        assertThat(mapping.toVirtual(ROUTE_B, 2)).isEqualTo(5);
    }

    @Test
    void shouldProduceUniqueVirtualIds() {
        var mapping = new BijectiveNodeIdMapping(Map.of(ROUTE_A, 0, ROUTE_B, 1, ROUTE_C, 2), 3);
        var virtualIds = new java.util.HashSet<Integer>();
        for (int t = 0; t < 10; t++) {
            for (String route : List.of(ROUTE_A, ROUTE_B, ROUTE_C)) {
                assertThat(virtualIds.add(mapping.toVirtual(route, t))).isTrue();
            }
        }
    }

    @Test
    void shouldRejectUnknownRoute() {
        var mapping = new BijectiveNodeIdMapping(Map.of(ROUTE_A, 0, ROUTE_B, 1), 2);
        assertThatThrownBy(() -> mapping.toVirtual("unknown", 0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldRejectTotalRoutesLessThanTwo() {
        var routes = Map.of(ROUTE_A, 0);
        assertThatThrownBy(() -> new BijectiveNodeIdMapping(routes, 1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldRejectIdOutOfRange() {
        var routes = Map.of(ROUTE_A, 0, ROUTE_B, 5);
        assertThatThrownBy(() -> new BijectiveNodeIdMapping(routes, 2))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("outside the valid range");
    }

    @Test
    void shouldHandleTargetNodeIdZero() {
        var mapping = new BijectiveNodeIdMapping(Map.of(ROUTE_A, 0, ROUTE_B, 1), 2);
        assertThat(mapping.toVirtual(ROUTE_A, 0)).isZero();
        assertThat(mapping.fromVirtual(0)).isEqualTo(new RouteAndNode(ROUTE_A, 0));
    }

    @Test
    void shouldThrowOnOverflow() {
        var mapping = new BijectiveNodeIdMapping(Map.of(ROUTE_A, 0, ROUTE_B, 1), 2);
        assertThatThrownBy(() -> mapping.toVirtual(ROUTE_A, Integer.MAX_VALUE))
                .isInstanceOf(ArithmeticException.class);
    }

    @Test
    void shouldPassThroughNegativeTargetNodeIds() {
        var mapping = new BijectiveNodeIdMapping(Map.of(ROUTE_A, 0, ROUTE_B, 1), 2);
        // Negative node IDs are Kafka protocol sentinels and must not be mapped.
        for (String route : List.of(ROUTE_A, ROUTE_B)) {
            assertThat(mapping.toVirtual(route, -1)).isEqualTo(-1);
            assertThat(mapping.toVirtual(route, -2)).isEqualTo(-2);
        }
    }

    @Test
    void shouldPassThroughNegativeVirtualNodeIdsInFromVirtual() {
        var mapping = new BijectiveNodeIdMapping(Map.of(ROUTE_A, 0, ROUTE_B, 1), 2);
        assertThat(mapping.fromVirtual(ROUTE_A, -1)).isEqualTo(-1);
        assertThat(mapping.fromVirtual(ROUTE_B, -2)).isEqualTo(-2);
    }

    @Test
    void shouldSupportNonContiguousIds() {
        var mapping = new BijectiveNodeIdMapping(Map.of(ROUTE_A, 0, ROUTE_B, 2), 3);
        assertThat(mapping.toVirtual(ROUTE_A, 0)).isZero();
        assertThat(mapping.toVirtual(ROUTE_B, 0)).isEqualTo(2);
        assertThat(mapping.toVirtual(ROUTE_A, 1)).isEqualTo(3);
        assertThat(mapping.toVirtual(ROUTE_B, 1)).isEqualTo(5);
    }
}
