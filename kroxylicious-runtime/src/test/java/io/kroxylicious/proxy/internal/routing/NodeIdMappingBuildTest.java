/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.routing;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.config.TargetCluster;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class NodeIdMappingBuildTest {

    private static RouteDescriptor clusterRoute(String name, int id) {
        return new RouteDescriptor(name, id, new TargetCluster("broker:9092", null), null, List.of());
    }

    @Test
    void shouldThrowOnEmptyRouteDescriptors() {
        // Given
        Map<String, RouteDescriptor> empty = Map.of();

        // When / Then
        assertThatThrownBy(() -> NodeIdMapping.build(empty))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("At least one");
    }

    @Test
    void shouldReturnIdentityMappingForSingleRoute() {
        // Given
        var routes = Map.of("only-route", clusterRoute("only-route", 0));

        // When
        NodeIdMapping mapping = NodeIdMapping.build(routes);

        // Then
        assertThat(mapping).isInstanceOf(IdentityNodeIdMapping.class);
    }

    @Test
    void shouldReturnBijectiveMappingForMultipleRoutes() {
        // Given
        var routes = Map.of(
                "r1", clusterRoute("r1", 0),
                "r2", clusterRoute("r2", 1));

        // When
        NodeIdMapping mapping = NodeIdMapping.build(routes);

        // Then
        assertThat(mapping).isInstanceOf(BijectiveNodeIdMapping.class);
    }

    @Test
    void shouldRoundTripCorrectlyForBuiltMapping() {
        // Given
        var routes = Map.of(
                "r1", clusterRoute("r1", 0),
                "r2", clusterRoute("r2", 1));
        NodeIdMapping mapping = NodeIdMapping.build(routes);

        // When / Then
        for (int targetNode = 0; targetNode < 10; targetNode++) {
            int virtual = mapping.toVirtual("r1", targetNode);
            int recovered = mapping.fromVirtual("r1", virtual);
            assertThat(recovered).isEqualTo(targetNode);
        }
    }
}
