/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.config;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.service.HostPort;

import static org.assertj.core.api.Assertions.assertThat;

class RoutingGraphWalkerTest {

    // -------------------------------------------------------------------------
    // anyInRouterGraph
    // -------------------------------------------------------------------------

    @Test
    void returnsFalseWhenNeitherPredicateMatches() {
        // Given
        var routers = Map.of("r1", routerDef("r1", "cluster-a"));
        var clusters = Map.of("cluster-a", clusterDef("cluster-a"));

        // When / Then
        assertThat(RoutingGraphWalker.anyInRouterGraph("r1", routers, clusters, name -> false, name -> false)).isFalse();
    }

    @Test
    void returnsTrueWhenRouterNamePredicateMatches() {
        // Given
        var routers = Map.of("r1", routerDef("r1", "cluster-a"));

        // When / Then
        assertThat(RoutingGraphWalker.anyInRouterGraph("r1", routers, Map.of(), "r1"::equals, name -> false)).isTrue();
    }

    @Test
    void returnsTrueWhenClusterLeafPredicateMatches() {
        // Given
        var routers = Map.of("r1", routerDef("r1", "cluster-a"));
        var clusters = Map.of("cluster-a", clusterDef("cluster-a"));

        // When / Then
        assertThat(RoutingGraphWalker.anyInRouterGraph("r1", routers, clusters, name -> false, "cluster-a"::equals)).isTrue();
    }

    @Test
    void returnsFalseWhenClusterLeafDoesNotMatchPredicate() {
        // Given
        var routers = Map.of("r1", routerDef("r1", "cluster-a"));
        var clusters = Map.of("cluster-a", clusterDef("cluster-a"));

        // When / Then
        assertThat(RoutingGraphWalker.anyInRouterGraph("r1", routers, clusters, name -> false, "cluster-b"::equals)).isFalse();
    }

    @Test
    void walksThroughNestedRouters() {
        // Given: r1 -> r2 -> cluster-deep
        var r2 = routerDef("r2", "cluster-deep");
        var r1 = routerDefWithRouterTarget("r1", "r2");
        var routers = Map.of("r1", r1, "r2", r2);
        var clusters = Map.of("cluster-deep", clusterDef("cluster-deep"));

        // When / Then
        assertThat(RoutingGraphWalker.anyInRouterGraph("r1", routers, clusters, name -> false, "cluster-deep"::equals)).isTrue();
    }

    @Test
    void returnsFalseForUnknownEntryPointRouter() {
        // When / Then
        assertThat(RoutingGraphWalker.anyInRouterGraph("unknown", Map.of(), Map.of(), name -> false, name -> false)).isFalse();
    }

    @Test
    void doesNotLoopOnCycle() {
        // Given: r1 -> r2 -> r1 (cycle)
        var r1 = routerDefWithRouterTarget("r1", "r2");
        var r2 = routerDefWithRouterTarget("r2", "r1");
        var routers = Map.of("r1", r1, "r2", r2);

        // When: neither predicate ever matches
        // Then: terminates without StackOverflowError
        assertThat(RoutingGraphWalker.anyInRouterGraph("r1", routers, Map.of(), name -> false, name -> false)).isFalse();
    }

    @Test
    void returnsTrueWhenNestedRouterNameMatchesPredicate() {
        // Given: r1 -> r2; r2 is in changedRouterNames
        var r2 = routerDef("r2", "cluster-a");
        var r1 = routerDefWithRouterTarget("r1", "r2");
        var routers = Map.of("r1", r1, "r2", r2);

        // When / Then
        assertThat(RoutingGraphWalker.anyInRouterGraph("r1", routers, Map.of(), "r2"::equals, name -> false)).isTrue();
    }

    // -------------------------------------------------------------------------
    // anyInClusterGraph
    // -------------------------------------------------------------------------

    @Test
    void vcWithNamedClusterReturnsTrueWhenClusterPredicateMatches() {
        // Given
        var vc = vcWithNamedCluster("vc1", "cluster-a");
        var clusters = Map.of("cluster-a", clusterDef("cluster-a"));

        // When / Then
        assertThat(RoutingGraphWalker.anyInClusterGraph(vc, Map.of(), clusters, name -> false, "cluster-a"::equals)).isTrue();
    }

    @Test
    void vcWithNamedClusterReturnsFalseWhenClusterPredicateDoesNotMatch() {
        // Given
        var vc = vcWithNamedCluster("vc1", "cluster-a");
        var clusters = Map.of("cluster-a", clusterDef("cluster-a"));

        // When / Then
        assertThat(RoutingGraphWalker.anyInClusterGraph(vc, Map.of(), clusters, name -> false, "cluster-b"::equals)).isFalse();
    }

    @Test
    void vcWithRouterReturnsTrueWhenRouterGraphContainsMatchingCluster() {
        // Given
        var routers = Map.of("r1", routerDef("r1", "cluster-a"));
        var clusters = Map.of("cluster-a", clusterDef("cluster-a"));
        var vc = vcWithRouter("vc1", "r1");

        // When / Then
        assertThat(RoutingGraphWalker.anyInClusterGraph(vc, routers, clusters, name -> false, "cluster-a"::equals)).isTrue();
    }

    @Test
    void vcWithRouterReturnsFalseWhenRouterGraphContainsNoMatchingCluster() {
        // Given
        var routers = Map.of("r1", routerDef("r1", "cluster-a"));
        var clusters = Map.of("cluster-a", clusterDef("cluster-a"));
        var vc = vcWithRouter("vc1", "r1");

        // When / Then
        assertThat(RoutingGraphWalker.anyInClusterGraph(vc, routers, clusters, name -> false, "cluster-b"::equals)).isFalse();
    }

    @Test
    @SuppressWarnings("deprecation")
    void vcWithInlineTargetClusterReturnsFalse() {
        // Given: inline targetCluster is not tracked by named-cluster detectors
        var vc = new VirtualCluster("vc1", new TargetCluster("kafka:9092", Optional.empty()),
                List.of(gateway()), false, false, null);

        // When / Then
        assertThat(RoutingGraphWalker.anyInClusterGraph(vc, Map.of(), Map.of(), name -> true, name -> true)).isFalse();
    }

    // -------------------------------------------------------------------------
    // walkClusterGraph — visitor is called for all node types
    // -------------------------------------------------------------------------

    @Test
    void walkClusterGraphCallsVisitVirtualCluster() {
        // Given
        var vc = vcWithNamedCluster("vc1", "cluster-a");

        // When
        var result = RoutingGraphWalker.walkClusterGraph(vc, Map.of(), Map.of(), () -> new RoutingGraphVisitor<List<VirtualCluster>>() {
            final List<VirtualCluster> visited = new ArrayList<>();

            @Override
            public boolean visitVirtualCluster(VirtualCluster v, WalkContext ctx) {
                visited.add(v);
                return true;
            }

            @Override
            public List<VirtualCluster> result() {
                return visited;
            }
        });

        // Then
        assertThat(result).containsExactly(vc);
    }

    @Test
    void walkClusterGraphCallsVisitClusterDefinitionForNamedCluster() {
        // Given
        var cd = clusterDef("cluster-a");
        var vc = vcWithNamedCluster("vc1", "cluster-a");

        // When
        var result = RoutingGraphWalker.walkClusterGraph(vc, Map.of(), Map.of("cluster-a", cd), () -> new RoutingGraphVisitor<List<ClusterDefinition>>() {
            final List<ClusterDefinition> visited = new ArrayList<>();

            @Override
            public boolean visitClusterName(ClusterDefinition c, WalkContext ctx) {
                visited.add(c);
                return true;
            }

            @Override
            public List<ClusterDefinition> result() {
                return visited;
            }
        });

        // Then
        assertThat(result).containsExactly(cd);
    }

    @Test
    void walkClusterGraphPassesNullClusterDefinitionWhenClusterNotInMap() {
        // Given: cluster name is referenced but not in clustersByName
        var vc = vcWithNamedCluster("vc1", "cluster-a");

        // When
        var result = RoutingGraphWalker.walkClusterGraph(vc, Map.of(), Map.of(), () -> new RoutingGraphVisitor<List<ClusterDefinition>>() {
            final List<ClusterDefinition> visited = new ArrayList<>();

            @Override
            public boolean visitClusterName(ClusterDefinition c, WalkContext ctx) {
                visited.add(c);
                return true;
            }

            @Override
            public List<ClusterDefinition> result() {
                return visited;
            }
        });

        // Then
        assertThat(result).containsExactly((ClusterDefinition) null);
    }

    @Test
    void walkClusterGraphCallsEnterRouterForRouterTarget() {
        // Given
        var rd = routerDef("r1", "cluster-a");
        var vc = vcWithRouter("vc1", "r1");

        // When
        var result = RoutingGraphWalker.walkClusterGraph(vc, Map.of("r1", rd), Map.of(), () -> new RoutingGraphVisitor<List<RouterDefinition>>() {
            final List<RouterDefinition> visited = new ArrayList<>();

            @Override
            public boolean enterRouter(RouterDefinition r, WalkContext ctx) {
                visited.add(r);
                return true;
            }

            @Override
            public List<RouterDefinition> result() {
                return visited;
            }
        });

        // Then
        assertThat(result).containsExactly(rd);
    }

    @Test
    void walkClusterGraphSkipsChildNodesWhenVisitVirtualClusterReturnsFalse() {
        // Given
        var vc = vcWithNamedCluster("vc1", "cluster-a");

        // When
        var result = RoutingGraphWalker.walkClusterGraph(vc, Map.of(), Map.of("cluster-a", clusterDef("cluster-a")),
                () -> new RoutingGraphVisitor<List<ClusterDefinition>>() {
                    final List<ClusterDefinition> clustersVisited = new ArrayList<>();

                    @Override
                    public boolean visitVirtualCluster(VirtualCluster v, WalkContext ctx) {
                        return false;
                    }

                    @Override
                    public boolean visitClusterName(ClusterDefinition c, WalkContext ctx) {
                        clustersVisited.add(c);
                        return true;
                    }

                    @Override
                    public List<ClusterDefinition> result() {
                        return clustersVisited;
                    }
                });

        // Then
        assertThat(result).isEmpty();
    }

    @Test
    void walkClusterGraphStopsAtClusterWhenVisitClusterNameReturnsFalse() {
        // Given: vc with named cluster; visitor stops at the cluster
        var cd = clusterDef("cluster-a");
        var vc = vcWithNamedCluster("vc1", "cluster-a");

        // When
        var result = RoutingGraphWalker.walkClusterGraph(vc, Map.of(), Map.of("cluster-a", cd),
                () -> new RoutingGraphVisitor<List<ClusterDefinition>>() {
                    final List<ClusterDefinition> clustersVisited = new ArrayList<>();

                    @Override
                    public boolean visitClusterName(ClusterDefinition c, WalkContext ctx) {
                        clustersVisited.add(c);
                        return false;
                    }

                    @Override
                    public List<ClusterDefinition> result() {
                        return clustersVisited;
                    }
                });

        // Then: the cluster was visited (the stop is signalled after visiting it)
        assertThat(result).containsExactly(cd);
    }

    @Test
    void walkClusterGraphVisitsAllRoutersWhenTraversalCompletes() {
        // Given: r1 -> r2 -> cluster-deep
        var r2 = routerDef("r2", "cluster-deep");
        var r1 = routerDefWithRouterTarget("r1", "r2");
        var routers = Map.of("r1", r1, "r2", r2);
        var vc = vcWithRouter("vc1", "r1");

        // When
        var result = RoutingGraphWalker.walkClusterGraph(vc, routers, Map.of(), () -> new RoutingGraphVisitor<List<RouterDefinition>>() {
            final List<RouterDefinition> routersVisited = new ArrayList<>();

            @Override
            public boolean enterRouter(RouterDefinition rd, WalkContext ctx) {
                if (rd != null && ctx.isFirstVisit()) {
                    routersVisited.add(rd);
                }
                return true;
            }

            @Override
            public List<RouterDefinition> result() {
                return routersVisited;
            }
        });

        // Then
        assertThat(result).containsExactlyInAnyOrder(r1, r2);
    }

    // -------------------------------------------------------------------------
    // walkRouterGraph — visitor is called for routers and cluster leaves
    // -------------------------------------------------------------------------

    @Test
    void walkRouterGraphVisitsRouterDefinitionAndClusterDefinition() {
        // Given
        var rd = routerDef("r1", "cluster-a");
        var cd = clusterDef("cluster-a");
        var routers = Map.of("r1", rd);
        var clusters = Map.of("cluster-a", cd);

        // When
        record Visited(List<RouterDefinition> routers, List<ClusterDefinition> clusters) {}
        var result = RoutingGraphWalker.walkRouterGraph("r1", routers, clusters, () -> new RoutingGraphVisitor<Visited>() {
            final List<RouterDefinition> routersVisited = new ArrayList<>();
            final List<ClusterDefinition> clustersVisited = new ArrayList<>();

            @Override
            public boolean enterRouter(RouterDefinition r, WalkContext ctx) {
                routersVisited.add(r);
                return true;
            }

            @Override
            public boolean visitClusterName(ClusterDefinition c, WalkContext ctx) {
                clustersVisited.add(c);
                return true;
            }

            @Override
            public Visited result() {
                return new Visited(routersVisited, clustersVisited);
            }
        });

        // Then
        assertThat(result.routers()).containsExactly(rd);
        assertThat(result.clusters()).containsExactly(cd);
    }

    @Test
    void walkRouterGraphVisitsAllNodesInNestedRouterChain() {
        // Given: r1 -> r2 -> cluster-deep
        var r2 = routerDef("r2", "cluster-deep");
        var r1 = routerDefWithRouterTarget("r1", "r2");
        var routers = Map.of("r1", r1, "r2", r2);
        var cd = clusterDef("cluster-deep");
        var clusters = Map.of("cluster-deep", cd);

        // When
        record Visited(List<RouterDefinition> routers, List<ClusterDefinition> clusters) {}
        var result = RoutingGraphWalker.walkRouterGraph("r1", routers, clusters, () -> new RoutingGraphVisitor<Visited>() {
            final List<RouterDefinition> routersVisited = new ArrayList<>();
            final List<ClusterDefinition> clustersVisited = new ArrayList<>();

            @Override
            public boolean enterRouter(RouterDefinition r, WalkContext ctx) {
                routersVisited.add(r);
                return true;
            }

            @Override
            public boolean visitClusterName(ClusterDefinition c, WalkContext ctx) {
                clustersVisited.add(c);
                return true;
            }

            @Override
            public Visited result() {
                return new Visited(routersVisited, clustersVisited);
            }
        });

        // Then
        assertThat(result.routers()).containsExactlyInAnyOrder(r1, r2);
        assertThat(result.clusters()).containsExactly(cd);
    }

    @Test
    void walkRouterGraphSkipsSubgraphWhenEnterRouterReturnsFalse() {
        // Given: r1 -> r2 -> cluster-deep; visitor stops at r1
        var r2 = routerDef("r2", "cluster-deep");
        var r1 = routerDefWithRouterTarget("r1", "r2");
        var routers = Map.of("r1", r1, "r2", r2);
        var cd = clusterDef("cluster-deep");

        // When
        record Visited(List<RouterDefinition> routers, List<ClusterDefinition> clusters) {}
        var result = RoutingGraphWalker.walkRouterGraph("r1", routers, Map.of("cluster-deep", cd), () -> new RoutingGraphVisitor<Visited>() {
            final List<RouterDefinition> routersVisited = new ArrayList<>();
            final List<ClusterDefinition> clustersVisited = new ArrayList<>();

            @Override
            public boolean enterRouter(RouterDefinition r, WalkContext ctx) {
                routersVisited.add(r);
                return false; // stop after the first router
            }

            @Override
            public boolean visitClusterName(ClusterDefinition c, WalkContext ctx) {
                clustersVisited.add(c);
                return true;
            }

            @Override
            public Visited result() {
                return new Visited(routersVisited, clustersVisited);
            }
        });

        // Then
        assertThat(result.routers()).containsExactly(r1);
        assertThat(result.clusters()).isEmpty();
    }

    @Test
    void walkRouterGraphStopsAtFirstClusterWhenVisitClusterNameReturnsFalse() {
        // Given: r1 has two cluster routes; visitor stops on first
        var r1 = new RouterDefinition("r1", "SomeRouterType", "cfg", List.of(
                new RouteDefinition("route1", 0, null, new RouteTarget("cluster-a", null)),
                new RouteDefinition("route2", 1, null, new RouteTarget("cluster-b", null))));
        var routers = Map.of("r1", r1);
        var cdA = clusterDef("cluster-a");
        var cdB = clusterDef("cluster-b");
        var clusters = Map.of("cluster-a", cdA, "cluster-b", cdB);

        // When
        var result = RoutingGraphWalker.walkRouterGraph("r1", routers, clusters, () -> new RoutingGraphVisitor<List<ClusterDefinition>>() {
            final List<ClusterDefinition> clustersVisited = new ArrayList<>();

            @Override
            public boolean visitClusterName(ClusterDefinition c, WalkContext ctx) {
                clustersVisited.add(c);
                return false;
            }

            @Override
            public List<ClusterDefinition> result() {
                return clustersVisited;
            }
        });

        // Then
        assertThat(result).containsExactly(cdA);
    }

    @Test
    void walkRouterGraphPassesNullDefinitionForUnknownRouter() {
        // When
        var result = RoutingGraphWalker.walkRouterGraph("unknown", Map.of(), Map.of(), () -> new RoutingGraphVisitor<List<RouterDefinition>>() {
            final List<RouterDefinition> visited = new ArrayList<>();

            @Override
            public boolean enterRouter(RouterDefinition rd, WalkContext ctx) {
                visited.add(rd);
                return true;
            }

            @Override
            public List<RouterDefinition> result() {
                return visited;
            }
        });

        // Then
        assertThat(result).containsExactly((RouterDefinition) null);
    }

    @Test
    void walkRouterGraphDoesNotLoopOnCycle() {
        // Given: r1 -> r2 -> r1 (cycle)
        var r1 = routerDefWithRouterTarget("r1", "r2");
        var r2 = routerDefWithRouterTarget("r2", "r1");
        var routers = Map.of("r1", r1, "r2", r2);

        // When: traversal terminates without StackOverflowError
        var result = RoutingGraphWalker.walkRouterGraph("r1", routers, Map.of(), () -> new RoutingGraphVisitor<List<RouterDefinition>>() {
            final List<RouterDefinition> routersVisited = new ArrayList<>();

            @Override
            public boolean enterRouter(RouterDefinition rd, WalkContext ctx) {
                if (rd != null && ctx.isFirstVisit()) {
                    routersVisited.add(rd);
                }
                return true;
            }

            @Override
            public List<RouterDefinition> result() {
                return routersVisited;
            }
        });

        // Then: each router visited as first-visit exactly once
        assertThat(result).containsExactlyInAnyOrder(r1, r2);
    }

    // -------------------------------------------------------------------------
    // WalkContext — path, edge context, and revisit detection
    // -------------------------------------------------------------------------

    @Test
    void walkContextPathContainsCorrectRouterSequenceForChain() {
        // Given: r1 -> r2 -> cluster-deep
        var r2 = routerDef("r2", "cluster-deep");
        var r1 = routerDefWithRouterTarget("r1", "r2");
        var routers = Map.of("r1", r1, "r2", r2);

        // When
        record Step(String routerName, List<String> path) {}
        var result = RoutingGraphWalker.walkRouterGraph("r1", routers, Map.of("cluster-deep", clusterDef("cluster-deep")),
                () -> new RoutingGraphVisitor<List<Step>>() {
                    final List<Step> steps = new ArrayList<>();

                    @Override
                    public boolean enterRouter(RouterDefinition rd, WalkContext ctx) {
                        if (rd != null) {
                            steps.add(new Step(rd.name(), ctx.path()));
                        }
                        return true;
                    }

                    @Override
                    public List<Step> result() {
                        return steps;
                    }
                });

        // Then
        assertThat(result).hasSize(2);
        assertThat(result.get(0).routerName()).isEqualTo("r1");
        assertThat(result.get(0).path()).containsExactly("r1");
        assertThat(result.get(1).routerName()).isEqualTo("r2");
        assertThat(result.get(1).path()).containsExactly("r1", "r2");
    }

    @Test
    void walkContextCarriesEdgeContextForClusterVisit() {
        // Given: r1 has a route "route1" targeting cluster-a
        var rd = routerDef("r1", "cluster-a");
        var cd = clusterDef("cluster-a");

        // When
        record ClusterStep(RouteDefinition route, RouterDefinition router) {}
        var result = RoutingGraphWalker.walkRouterGraph("r1", Map.of("r1", rd), Map.of("cluster-a", cd),
                () -> new RoutingGraphVisitor<List<ClusterStep>>() {
                    final List<ClusterStep> steps = new ArrayList<>();

                    @Override
                    public boolean visitClusterName(ClusterDefinition c, WalkContext ctx) {
                        steps.add(new ClusterStep(ctx.currentRoute(), ctx.sourceRouter()));
                        return true;
                    }

                    @Override
                    public List<ClusterStep> result() {
                        return steps;
                    }
                });

        // Then
        assertThat(result).hasSize(1);
        assertThat(result.get(0).router()).isEqualTo(rd);
        assertThat(result.get(0).route().cluster()).isEqualTo("cluster-a");
    }

    @Test
    void walkContextIsFirstVisitFalseAndPathShowsFullCycleOnRevisit() {
        // Given: r1 -> r2 -> r1 (cycle)
        var r1 = routerDefWithRouterTarget("r1", "r2");
        var r2 = routerDefWithRouterTarget("r2", "r1");
        var routers = Map.of("r1", r1, "r2", r2);

        // When
        record RevisitStep(boolean isFirstVisit, List<String> path) {}
        var result = RoutingGraphWalker.walkRouterGraph("r1", routers, Map.of(),
                () -> new RoutingGraphVisitor<List<RevisitStep>>() {
                    final List<RevisitStep> revisits = new ArrayList<>();

                    @Override
                    public boolean enterRouter(RouterDefinition rd, WalkContext ctx) {
                        if (!ctx.isFirstVisit()) {
                            revisits.add(new RevisitStep(ctx.isFirstVisit(), ctx.path()));
                        }
                        return true;
                    }

                    @Override
                    public List<RevisitStep> result() {
                        return revisits;
                    }
                });

        // Then: exactly one revisit, path shows the full cycle r1 -> r2 -> r1
        assertThat(result).hasSize(1);
        assertThat(result.get(0).isFirstVisit()).isFalse();
        assertThat(result.get(0).path()).containsExactly("r1", "r2", "r1");
    }

    @Test
    void walkContextEntryPointHasNullRouteAndRouter() {
        // Given: single router with a cluster route
        var rd = routerDef("r1", "cluster-a");

        // When
        var result = RoutingGraphWalker.walkRouterGraph("r1", Map.of("r1", rd), Map.of(),
                () -> new RoutingGraphVisitor<List<WalkContext>>() {
                    final List<WalkContext> contexts = new ArrayList<>();

                    @Override
                    public boolean enterRouter(RouterDefinition r, WalkContext ctx) {
                        contexts.add(ctx);
                        return true;
                    }

                    @Override
                    public List<WalkContext> result() {
                        return contexts;
                    }
                });

        // Then: entry-point router has null currentRoute and sourceRouter
        assertThat(result).hasSize(1);
        assertThat(result.get(0).currentRoute()).isNull();
        assertThat(result.get(0).sourceRouter()).isNull();
        assertThat(result.get(0).isFirstVisit()).isTrue();
        assertThat(result.get(0).path()).containsExactly("r1");
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static RouterDefinition routerDef(String name, String clusterTarget) {
        var route = new RouteDefinition("route", 0, null, new RouteTarget(clusterTarget, null));
        return new RouterDefinition(name, "SomeRouterType", "cfg", List.of(route));
    }

    private static RouterDefinition routerDefWithRouterTarget(String name, String routerTarget) {
        var route = new RouteDefinition("route", 0, null, new RouteTarget(null, routerTarget));
        return new RouterDefinition(name, "SomeRouterType", "cfg", List.of(route));
    }

    private static ClusterDefinition clusterDef(String name) {
        return new ClusterDefinition(name, "kafka:9092", null);
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
