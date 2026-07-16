/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.config;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.function.Supplier;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Walks the routing graph rooted at a {@link VirtualCluster} (or a named router),
 * notifying a {@link RoutingGraphVisitor} at each node.
 * <p>
 * A virtual cluster always has a routing graph: either a trivial one (a single named
 * cluster) or a router DAG. Each concern can be expressed as a {@link RoutingGraphVisitor}
 * rather than re-implementing traversal.
 * <p>
 * The walker detects revisits (cycles) by tracking two sets internally: {@code visited}
 * (fully-explored routers, used to skip cross-edges) and {@code inPath} (routers on the
 * current DFS path, used to detect revisits). When a router already in {@code inPath} is
 * encountered, {@link RoutingGraphVisitor#enterRouter} is called with
 * {@link WalkContext#isFirstVisit()} {@code false} and the walker does not recurse further.
 */
public final class RoutingGraphWalker {

    private RoutingGraphWalker() {
    }

    /**
     * Walks the full routing graph reachable from {@code vc} and returns the visitor's result.
     * <p>
     * A fresh visitor is obtained from {@code visitorFactory} at the start of each call.
     *
     * @param vc             the virtual cluster whose graph is walked
     * @param routersByName  all router definitions indexed by name
     * @param clustersByName all cluster definitions indexed by name
     * @param visitorFactory produces a new, ready-to-use visitor for this walk
     * @param <T>            the result type produced by the visitor
     * @return the value returned by {@link RoutingGraphVisitor#result()} after the walk
     */
    public static <T> T walkClusterGraph(VirtualCluster vc,
                                         Map<String, RouterDefinition> routersByName,
                                         Map<String, ClusterDefinition> clustersByName,
                                         Supplier<RoutingGraphVisitor<T>> visitorFactory) {
        var visitor = visitorFactory.get();
        traverseClusterGraph(vc, routersByName, clustersByName, visitor);
        return visitor.result();
    }

    /**
     * Walks the router DAG reachable from {@code routerName} and returns the visitor's result.
     * <p>
     * A fresh visitor is obtained from {@code visitorFactory} at the start of each call.
     *
     * @param routerName     name of the entry-point router
     * @param routersByName  all router definitions indexed by name
     * @param clustersByName all cluster definitions indexed by name
     * @param visitorFactory produces a new, ready-to-use visitor for this walk
     * @param <T>            the result type produced by the visitor
     * @return the value returned by {@link RoutingGraphVisitor#result()} after the walk
     */
    public static <T> T walkRouterGraph(String routerName,
                                        Map<String, RouterDefinition> routersByName,
                                        Map<String, ClusterDefinition> clustersByName,
                                        Supplier<RoutingGraphVisitor<T>> visitorFactory) {
        var visitor = visitorFactory.get();
        traverseRouterGraph(routerName, null, null, routersByName, clustersByName, visitor);
        return visitor.result();
    }

    /**
     * Returns {@code true} if any node reachable from {@code vc} in its routing graph
     * satisfies either predicate.
     *
     * @param vc                   the virtual cluster whose graph is walked
     * @param routersByName        all router definitions indexed by name
     * @param clustersByName       all cluster definitions indexed by name
     * @param routerNamePredicate  test applied to each visited router definition's name
     * @param clusterNamePredicate test applied to each cluster definition's name at a leaf
     */
    public static boolean anyInClusterGraph(VirtualCluster vc,
                                            Map<String, RouterDefinition> routersByName,
                                            Map<String, ClusterDefinition> clustersByName,
                                            Predicate<String> routerNamePredicate,
                                            Predicate<String> clusterNamePredicate) {
        return walkClusterGraph(vc, routersByName, clustersByName,
                () -> new AnyMatchVisitor(routerNamePredicate, clusterNamePredicate));
    }

    /**
     * Returns {@code true} if any node reachable from {@code routerName} in the router DAG
     * satisfies either predicate.
     *
     * @param routerName           name of the entry-point router
     * @param routersByName        all router definitions indexed by name
     * @param clustersByName       all cluster definitions indexed by name
     * @param routerNamePredicate  test applied to each visited router definition's name
     * @param clusterNamePredicate test applied to each cluster definition's name at a leaf
     */
    public static boolean anyInRouterGraph(String routerName,
                                           Map<String, RouterDefinition> routersByName,
                                           Map<String, ClusterDefinition> clustersByName,
                                           Predicate<String> routerNamePredicate,
                                           Predicate<String> clusterNamePredicate) {
        return walkRouterGraph(routerName, routersByName, clustersByName,
                () -> new AnyMatchVisitor(routerNamePredicate, clusterNamePredicate));
    }

    private static boolean traverseClusterGraph(VirtualCluster vc,
                                                Map<String, RouterDefinition> routersByName,
                                                Map<String, ClusterDefinition> clustersByName,
                                                RoutingGraphVisitor<?> visitor) {
        WalkContext entryCtx = new WalkContext(null, null, true, List.of());
        if (!visitor.visitVirtualCluster(vc, entryCtx)) {
            return false;
        }
        String namedCluster = vc.namedTargetCluster();
        if (namedCluster != null) {
            WalkContext clusterCtx = new WalkContext(null, null, true, List.of());
            return visitor.visitClusterName(clustersByName.get(namedCluster), clusterCtx);
        }
        String router = vc.router();
        if (router != null) {
            return traverseRouterGraph(router, null, null, routersByName, clustersByName, visitor);
        }
        return true;
    }

    private static boolean traverseRouterGraph(String routerName,
                                               @Nullable RouteDefinition incomingRoute,
                                               @Nullable RouterDefinition sourceRouter,
                                               Map<String, RouterDefinition> routersByName,
                                               Map<String, ClusterDefinition> clustersByName,
                                               RoutingGraphVisitor<?> visitor) {
        return traverseGraph(routerName, incomingRoute, sourceRouter, routersByName, clustersByName, visitor,
                new HashSet<>(), new HashSet<>(), new ArrayList<>());
    }

    private static boolean traverseGraph(String routerName,
                                         @Nullable RouteDefinition incomingRoute,
                                         @Nullable RouterDefinition sourceRouter,
                                         Map<String, RouterDefinition> routersByName,
                                         Map<String, ClusterDefinition> clustersByName,
                                         RoutingGraphVisitor<?> visitor,
                                         Set<String> visited,
                                         Set<String> inPath,
                                         List<String> currentPath) {
        if (visited.contains(routerName)) {
            // Cross-edge: already fully explored via another path — skip silently.
            return true;
        }

        RouterDefinition rd = routersByName.get(routerName);
        boolean addedToInPath = inPath.add(routerName);
        boolean firstVisit = addedToInPath;

        currentPath.add(routerName);
        WalkContext ctx = new WalkContext(incomingRoute, sourceRouter, firstVisit, List.copyOf(currentPath));

        if (!visitor.enterRouter(rd, ctx)) {
            currentPath.remove(currentPath.size() - 1);
            if (addedToInPath) {
                inPath.remove(routerName);
            }
            return false;
        }

        if (!firstVisit) {
            // Revisit: already on the DFS path (cycle). Don't recurse.
            currentPath.remove(currentPath.size() - 1);
            return true;
        }

        // Fresh node: recurse into routes.
        boolean result = true;
        if (rd != null) {
            outer: for (var route : rd.routes()) {
                if (route.cluster() != null) {
                    ClusterDefinition cd = clustersByName.get(route.cluster());
                    WalkContext clusterCtx = new WalkContext(route, rd, true, List.copyOf(currentPath));
                    if (!visitor.visitClusterName(cd, clusterCtx)) {
                        result = false;
                        break outer;
                    }
                }
                if (route.router() != null) {
                    if (!traverseGraph(route.router(), route, rd, routersByName, clustersByName, visitor, visited, inPath, currentPath)) {
                        result = false;
                        break outer;
                    }
                }
            }
        }

        currentPath.remove(currentPath.size() - 1);
        inPath.remove(routerName);
        visited.add(routerName);
        return result;
    }

    private static final class AnyMatchVisitor implements RoutingGraphVisitor<Boolean> {

        private final Predicate<String> routerNamePredicate;
        private final Predicate<String> clusterNamePredicate;
        private boolean found;

        AnyMatchVisitor(Predicate<String> routerNamePredicate, Predicate<String> clusterNamePredicate) {
            this.routerNamePredicate = routerNamePredicate;
            this.clusterNamePredicate = clusterNamePredicate;
        }

        @Override
        public boolean enterRouter(@Nullable RouterDefinition rd, WalkContext ctx) {
            if (rd != null && ctx.isFirstVisit() && routerNamePredicate.test(rd.name())) {
                found = true;
                return false;
            }
            return true;
        }

        @Override
        public boolean visitClusterName(@Nullable ClusterDefinition cd, WalkContext ctx) {
            if (cd != null && clusterNamePredicate.test(cd.name())) {
                found = true;
                return false;
            }
            return true;
        }

        @Override
        public Boolean result() {
            return found;
        }
    }
}
