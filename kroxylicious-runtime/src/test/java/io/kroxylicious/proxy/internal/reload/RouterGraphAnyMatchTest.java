/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.reload;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.config.RouteDefinition;
import io.kroxylicious.proxy.config.RouteTarget;
import io.kroxylicious.proxy.config.RouterDefinition;

import static org.assertj.core.api.Assertions.assertThat;

class RouterGraphAnyMatchTest {

    @Test
    void returnsFalseWhenNeitherPredicateMatches() {
        // Given
        var rd = routerDef("r1", "cluster-a");
        var routers = Map.of("r1", rd);

        // When / Then
        assertThat(RouterGraphAnyMatch.anyInRouterGraph("r1", routers, name -> false, name -> false)).isFalse();
    }

    @Test
    void returnsTrueWhenRouterNamePredicateMatches() {
        // Given
        var rd = routerDef("r1", "cluster-a");
        var routers = Map.of("r1", rd);

        // When / Then
        assertThat(RouterGraphAnyMatch.anyInRouterGraph("r1", routers, "r1"::equals, name -> false)).isTrue();
    }

    @Test
    void returnsTrueWhenClusterLeafPredicateMatches() {
        // Given: router r1 has a route targeting cluster-a
        var rd = routerDef("r1", "cluster-a");
        var routers = Map.of("r1", rd);

        // When / Then
        assertThat(RouterGraphAnyMatch.anyInRouterGraph("r1", routers, name -> false, "cluster-a"::equals)).isTrue();
    }

    @Test
    void returnsFalseWhenClusterLeafDoesNotMatchPredicate() {
        // Given
        var rd = routerDef("r1", "cluster-a");
        var routers = Map.of("r1", rd);

        // When / Then
        assertThat(RouterGraphAnyMatch.anyInRouterGraph("r1", routers, name -> false, "cluster-b"::equals)).isFalse();
    }

    @Test
    void walksThroughNestedRouters() {
        // Given: r1 -> r2 -> cluster-deep
        var r2 = routerDef("r2", "cluster-deep");
        var r1 = routerDefWithRouterTarget("r1", "r2");
        var routers = Map.of("r1", r1, "r2", r2);

        // When / Then
        assertThat(RouterGraphAnyMatch.anyInRouterGraph("r1", routers, name -> false, "cluster-deep"::equals)).isTrue();
    }

    @Test
    void returnsFalseForUnknownEntryPointRouter() {
        // Given: empty router map
        // When / Then
        assertThat(RouterGraphAnyMatch.anyInRouterGraph("unknown", Map.of(), name -> false, name -> false)).isFalse();
    }

    @Test
    void doesNotLoopOnCycle() {
        // Given: r1 -> r2 -> r1 (cycle)
        var r1 = routerDefWithRouterTarget("r1", "r2");
        var r2 = routerDefWithRouterTarget("r2", "r1");
        var routers = Map.of("r1", r1, "r2", r2);

        // When: neither predicate ever matches
        // Then: terminates without StackOverflowError
        assertThat(RouterGraphAnyMatch.anyInRouterGraph("r1", routers, name -> false, name -> false)).isFalse();
    }

    @Test
    void returnsTrueWhenNestedRouterNameMatchesPredicate() {
        // Given: r1 -> r2; r2 is in changedRouterNames
        var r2 = routerDef("r2", "cluster-a");
        var r1 = routerDefWithRouterTarget("r1", "r2");
        var routers = Map.of("r1", r1, "r2", r2);

        // When / Then
        assertThat(RouterGraphAnyMatch.anyInRouterGraph("r1", routers, Set.of("r2")::contains, name -> false)).isTrue();
    }

    // ---- fixture helpers ----

    private static RouterDefinition routerDef(String name, String clusterTarget) {
        var route = new RouteDefinition("route", 0, null, new RouteTarget(clusterTarget, null));
        return new RouterDefinition(name, "SomeRouterType", "cfg", List.of(route));
    }

    private static RouterDefinition routerDefWithRouterTarget(String name, String routerTarget) {
        var route = new RouteDefinition("route", 0, null, new RouteTarget(null, routerTarget));
        return new RouterDefinition(name, "SomeRouterType", "cfg", List.of(route));
    }
}
