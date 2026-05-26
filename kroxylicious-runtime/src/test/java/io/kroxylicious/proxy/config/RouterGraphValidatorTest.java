/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.config;

import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RouterGraphValidatorTest {

    private static RouteDefinition clusterRoute(String name, int id, String cluster) {
        return new RouteDefinition(name, id, null, new RouteDefinition.Target(cluster, null));
    }

    private static RouteDefinition routerRoute(String name, int id, String router) {
        return new RouteDefinition(name, id, null, new RouteDefinition.Target(null, router));
    }

    private static RouterDefinition router(String name, RouteDefinition... routes) {
        return new RouterDefinition(name, "SomeType", null, List.of(routes));
    }

    @Test
    void shouldAcceptSingleRouterWithClusterRoute() {
        var routers = List.of(router("r1", clusterRoute("foo", 0, "c1")));
        assertThatCode(() -> RouterGraphValidator.validate(routers, Set.of("c1")))
                .doesNotThrowAnyException();
    }

    @Test
    void shouldAcceptLinearChain() {
        var routers = List.of(
                router("r1", routerRoute("next", 0, "r2")),
                router("r2", clusterRoute("foo", 0, "c1")));
        assertThatCode(() -> RouterGraphValidator.validate(routers, Set.of("c1")))
                .doesNotThrowAnyException();
    }

    @Test
    void shouldAcceptDiamondDag() {
        var routers = List.of(
                router("r1", routerRoute("left", 0, "r2"), routerRoute("right", 1, "r3")),
                router("r2", clusterRoute("foo", 0, "c1")),
                router("r3", clusterRoute("bar", 0, "c1")));
        assertThatCode(() -> RouterGraphValidator.validate(routers, Set.of("c1")))
                .doesNotThrowAnyException();
    }

    @Test
    void shouldRejectSelfCycle() {
        var routers = List.of(router("r1", routerRoute("loop", 0, "r1")));
        assertThatThrownBy(() -> RouterGraphValidator.validate(routers, Set.of()))
                .isInstanceOf(IllegalConfigurationException.class)
                .hasMessageContaining("cycle")
                .hasMessageContaining("r1");
    }

    @Test
    void shouldRejectSimpleCycle() {
        var routers = List.of(
                router("r1", routerRoute("next", 0, "r2")),
                router("r2", routerRoute("back", 0, "r1")));
        assertThatThrownBy(() -> RouterGraphValidator.validate(routers, Set.of()))
                .isInstanceOf(IllegalConfigurationException.class)
                .hasMessageContaining("cycle")
                .hasMessageContaining("r1")
                .hasMessageContaining("r2");
    }

    @Test
    void shouldRejectThreeNodeCycle() {
        var routers = List.of(
                router("r1", routerRoute("next", 0, "r2")),
                router("r2", routerRoute("next", 0, "r3")),
                router("r3", routerRoute("back", 0, "r1")));
        assertThatThrownBy(() -> RouterGraphValidator.validate(routers, Set.of()))
                .isInstanceOf(IllegalConfigurationException.class)
                .hasMessageContaining("cycle");
    }

    @Test
    void shouldRejectDanglingClusterReference() {
        var routers = List.of(router("r1", clusterRoute("foo", 0, "nonexistent")));
        assertThatThrownBy(() -> RouterGraphValidator.validate(routers, Set.of("c1")))
                .isInstanceOf(IllegalConfigurationException.class)
                .hasMessageContaining("unknown cluster")
                .hasMessageContaining("nonexistent");
    }

    @Test
    void shouldRejectDanglingRouterReference() {
        var routers = List.of(router("r1", routerRoute("next", 0, "nonexistent")));
        assertThatThrownBy(() -> RouterGraphValidator.validate(routers, Set.of()))
                .isInstanceOf(IllegalConfigurationException.class)
                .hasMessageContaining("unknown router")
                .hasMessageContaining("nonexistent");
    }
}
