/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.routing;

import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.internal.routing.NodeIdMapping.RouteAndNode;

import static org.assertj.core.api.Assertions.assertThat;

class IdentityNodeIdMappingTest {

    private static final String ROUTE = "my-route";

    @Test
    void shouldReturnTargetIdUnchanged() {
        var mapping = new IdentityNodeIdMapping(ROUTE);
        for (int t = 0; t <= 100; t++) {
            assertThat(mapping.toVirtual(ROUTE, t)).isEqualTo(t);
        }
    }

    @Test
    void shouldReturnRouteNameForAnyVirtualId() {
        var mapping = new IdentityNodeIdMapping(ROUTE);
        for (int v = 0; v <= 100; v++) {
            assertThat(mapping.fromVirtual(v)).isEqualTo(new RouteAndNode(ROUTE, v));
        }
    }

    @Test
    void shouldIgnoreRouteParameterInToVirtual() {
        var mapping = new IdentityNodeIdMapping(ROUTE);
        assertThat(mapping.toVirtual("any-route", 42)).isEqualTo(42);
    }
}
