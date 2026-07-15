/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.reload;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.config.PortIdentifiesNodeIdentificationStrategy;
import io.kroxylicious.proxy.config.RouteDefinition;
import io.kroxylicious.proxy.config.RouteTarget;
import io.kroxylicious.proxy.config.RouterDefinition;
import io.kroxylicious.proxy.config.TargetCluster;
import io.kroxylicious.proxy.config.VirtualCluster;
import io.kroxylicious.proxy.config.VirtualClusterGateway;
import io.kroxylicious.proxy.service.HostPort;

import static org.assertj.core.api.Assertions.assertThat;

class ClusterGraphTesterTest {

    @Test
    void returnsFalseWhenNeitherPredicateMatches() {
        // Given
        var routers = Map.of("r1", routerDef("r1", "cluster-a"));

        // When / Then
        assertThat(ClusterGraphTester.anyInRouterGraph("r1", routers, name -> false, name -> false)).isFalse();
    }

    @Test
    void returnsTrueWhenRouterNamePredicateMatches() {
        // Given
        var routers = Map.of("r1", routerDef("r1", "cluster-a"));

        // When / Then
        assertThat(ClusterGraphTester.anyInRouterGraph("r1", routers, "r1"::equals, name -> false)).isTrue();
    }

    @Test
    void returnsTrueWhenClusterLeafPredicateMatches() {
        // Given
        var routers = Map.of("r1", routerDef("r1", "cluster-a"));

        // When / Then
        assertThat(ClusterGraphTester.anyInRouterGraph("r1", routers, name -> false, "cluster-a"::equals)).isTrue();
    }

    @Test
    void returnsFalseWhenClusterLeafDoesNotMatchPredicate() {
        // Given
        var routers = Map.of("r1", routerDef("r1", "cluster-a"));

        // When / Then
        assertThat(ClusterGraphTester.anyInRouterGraph("r1", routers, name -> false, "cluster-b"::equals)).isFalse();
    }

    @Test
    void walksThroughNestedRouters() {
        // Given: r1 -> r2 -> cluster-deep
        var r2 = routerDef("r2", "cluster-deep");
        var r1 = routerDefWithRouterTarget("r1", "r2");
        var routers = Map.of("r1", r1, "r2", r2);

        // When / Then
        assertThat(ClusterGraphTester.anyInRouterGraph("r1", routers, name -> false, "cluster-deep"::equals)).isTrue();
    }

    @Test
    void returnsFalseForUnknownEntryPointRouter() {
        // When / Then
        assertThat(ClusterGraphTester.anyInRouterGraph("unknown", Map.of(), name -> false, name -> false)).isFalse();
    }

    @Test
    void doesNotLoopOnCycle() {
        // Given: r1 -> r2 -> r1 (cycle)
        var r1 = routerDefWithRouterTarget("r1", "r2");
        var r2 = routerDefWithRouterTarget("r2", "r1");
        var routers = Map.of("r1", r1, "r2", r2);

        // When: neither predicate ever matches
        // Then: terminates without StackOverflowError
        assertThat(ClusterGraphTester.anyInRouterGraph("r1", routers, name -> false, name -> false)).isFalse();
    }

    @Test
    void returnsTrueWhenNestedRouterNameMatchesPredicate() {
        // Given: r1 -> r2; r2 is in changedRouterNames
        var r2 = routerDef("r2", "cluster-a");
        var r1 = routerDefWithRouterTarget("r1", "r2");
        var routers = Map.of("r1", r1, "r2", r2);

        // When / Then
        assertThat(ClusterGraphTester.anyInRouterGraph("r1", routers, Set.of("r2")::contains, name -> false)).isTrue();
    }

    @Test
    void vcWithNamedClusterReturnsTrueWhenClusterPredicateMatches() {
        // Given
        var vc = vcWithNamedCluster("vc1", "cluster-a");

        // When / Then
        assertThat(ClusterGraphTester.anyInClusterGraph(vc, Map.of(), name -> false, "cluster-a"::equals)).isTrue();
    }

    @Test
    void vcWithNamedClusterReturnsFalseWhenClusterPredicateDoesNotMatch() {
        // Given
        var vc = vcWithNamedCluster("vc1", "cluster-a");

        // When / Then
        assertThat(ClusterGraphTester.anyInClusterGraph(vc, Map.of(), name -> false, "cluster-b"::equals)).isFalse();
    }

    @Test
    void vcWithRouterReturnsTrueWhenRouterGraphContainsMatchingCluster() {
        // Given
        var routers = Map.of("r1", routerDef("r1", "cluster-a"));
        var vc = vcWithRouter("vc1", "r1");

        // When / Then
        assertThat(ClusterGraphTester.anyInClusterGraph(vc, routers, name -> false, "cluster-a"::equals)).isTrue();
    }

    @Test
    void vcWithRouterReturnsFalseWhenRouterGraphContainsNoMatchingCluster() {
        // Given
        var routers = Map.of("r1", routerDef("r1", "cluster-a"));
        var vc = vcWithRouter("vc1", "r1");

        // When / Then
        assertThat(ClusterGraphTester.anyInClusterGraph(vc, routers, name -> false, "cluster-b"::equals)).isFalse();
    }

    @Test
    @SuppressWarnings("deprecation")
    void vcWithInlineTargetClusterReturnsFalse() {
        // Given: inline targetCluster is not tracked by named-cluster detectors
        var vc = new VirtualCluster("vc1", new TargetCluster("kafka:9092", Optional.empty()),
                List.of(gateway()), false, false, null);

        // When / Then
        assertThat(ClusterGraphTester.anyInClusterGraph(vc, Map.of(), name -> true, name -> true)).isFalse();
    }

    private static RouterDefinition routerDef(String name, String clusterTarget) {
        var route = new RouteDefinition("route", 0, null, new RouteTarget(clusterTarget, null));
        return new RouterDefinition(name, "SomeRouterType", "cfg", List.of(route));
    }

    private static RouterDefinition routerDefWithRouterTarget(String name, String routerTarget) {
        var route = new RouteDefinition("route", 0, null, new RouteTarget(null, routerTarget));
        return new RouterDefinition(name, "SomeRouterType", "cfg", List.of(route));
    }

    private static VirtualCluster vcWithNamedCluster(String name, String clusterName) {
        return new VirtualCluster(name, null, new RouteTarget(clusterName, null),
                List.of(gateway()), false, false, null, null, null, null);
    }

    private static VirtualCluster vcWithRouter(String name, String routerName) {
        return new VirtualCluster(name, null, new RouteTarget(null, routerName),
                List.of(gateway()), false, false, null, null, null, null);
    }

    private static VirtualClusterGateway gateway() {
        return new VirtualClusterGateway("default",
                new PortIdentifiesNodeIdentificationStrategy(new HostPort("localhost", 9192), null, null, null),
                null, Optional.empty());
    }
}
