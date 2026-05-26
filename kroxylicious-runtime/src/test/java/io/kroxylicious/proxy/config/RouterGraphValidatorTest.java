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

    private static RouteDefinition clusterRoute(String name, String cluster) {
        return new RouteDefinition(name, null, new RouteDefinition.Target(cluster, null));
    }

    private static RouteDefinition routerRoute(String name, String router) {
        return new RouteDefinition(name, null, new RouteDefinition.Target(null, router));
    }

    private static RouterDefinition router(String name, RouteDefinition... routes) {
        return new RouterDefinition(name, "SomeType", null, List.of(routes));
    }

    @Test
    void shouldAcceptSingleRouterWithClusterRoute() {
        var routers = List.of(router("r1", clusterRoute("foo", "c1")));
        assertThatCode(() -> RouterGraphValidator.validate(routers, Set.of("c1")))
                .doesNotThrowAnyException();
    }

    @Test
    void shouldAcceptLinearChain() {
        var routers = List.of(
                router("r1", routerRoute("next", "r2")),
                router("r2", clusterRoute("foo", "c1")));
        assertThatCode(() -> RouterGraphValidator.validate(routers, Set.of("c1")))
                .doesNotThrowAnyException();
    }

    @Test
    void shouldAcceptDiamondDag() {
        var routers = List.of(
                router("r1", routerRoute("left", "r2"), routerRoute("right", "r3")),
                router("r2", clusterRoute("foo", "c1")),
                router("r3", clusterRoute("bar", "c1")));
        assertThatCode(() -> RouterGraphValidator.validate(routers, Set.of("c1")))
                .doesNotThrowAnyException();
    }

    @Test
    void shouldRejectSelfCycle() {
        var routers = List.of(router("r1", routerRoute("loop", "r1")));
        assertThatThrownBy(() -> RouterGraphValidator.validate(routers, Set.of()))
                .isInstanceOf(IllegalConfigurationException.class)
                .hasMessageContaining("cycle")
                .hasMessageContaining("r1");
    }

    @Test
    void shouldRejectSimpleCycle() {
        var routers = List.of(
                router("r1", routerRoute("next", "r2")),
                router("r2", routerRoute("back", "r1")));
        assertThatThrownBy(() -> RouterGraphValidator.validate(routers, Set.of()))
                .isInstanceOf(IllegalConfigurationException.class)
                .hasMessageContaining("cycle")
                .hasMessageContaining("r1")
                .hasMessageContaining("r2");
    }

    @Test
    void shouldRejectThreeNodeCycle() {
        var routers = List.of(
                router("r1", routerRoute("next", "r2")),
                router("r2", routerRoute("next", "r3")),
                router("r3", routerRoute("back", "r1")));
        assertThatThrownBy(() -> RouterGraphValidator.validate(routers, Set.of()))
                .isInstanceOf(IllegalConfigurationException.class)
                .hasMessageContaining("cycle");
    }

    @Test
    void shouldRejectDanglingClusterReference() {
        var routers = List.of(router("r1", clusterRoute("foo", "nonexistent")));
        assertThatThrownBy(() -> RouterGraphValidator.validate(routers, Set.of("c1")))
                .isInstanceOf(IllegalConfigurationException.class)
                .hasMessageContaining("unknown cluster")
                .hasMessageContaining("nonexistent");
    }

    @Test
    void shouldRejectDanglingRouterReference() {
        var routers = List.of(router("r1", routerRoute("next", "nonexistent")));
        assertThatThrownBy(() -> RouterGraphValidator.validate(routers, Set.of()))
                .isInstanceOf(IllegalConfigurationException.class)
                .hasMessageContaining("unknown router")
                .hasMessageContaining("nonexistent");
    }
}
