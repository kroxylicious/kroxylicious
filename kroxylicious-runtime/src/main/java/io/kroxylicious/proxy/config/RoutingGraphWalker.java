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
    @Nullable
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
    @Nullable
    public static <T> T walkRouterGraph(String routerName,
                                        Map<String, RouterDefinition> routersByName,
                                        Map<String, ClusterDefinition> clustersByName,
                                        Supplier<RoutingGraphVisitor<T>> visitorFactory) {
        var visitor = visitorFactory.get();
        traverseRouterGraph(routerName, routersByName, clustersByName, visitor);
        return visitor.result();
    }

    private static void traverseClusterGraph(VirtualCluster vc,
                                             Map<String, RouterDefinition> routersByName,
                                             Map<String, ClusterDefinition> clustersByName,
                                             RoutingGraphVisitor<?> visitor) {
        WalkContext entryCtx = new WalkContext(null, null, true, List.of());
        if (!visitor.visitVirtualCluster(vc, entryCtx)) {
            return;
        }
        String namedCluster = vc.namedTargetCluster();
        if (namedCluster != null) {
            ClusterDefinition cd = clustersByName.get(namedCluster);
            if (cd != null) {
                WalkContext clusterCtx = new WalkContext(null, null, true, List.of());
                visitor.visitClusterDefinition(cd, clusterCtx);
            }
            return;
        }
        String router = vc.router();
        if (router != null) {
            traverseRouterGraph(router, routersByName, clustersByName, visitor);
        }
    }

    private static void traverseRouterGraph(String routerName,
                                            Map<String, RouterDefinition> routersByName,
                                            Map<String, ClusterDefinition> clustersByName,
                                            RoutingGraphVisitor<?> visitor) {
        traverseGraph(routerName, null, null, routersByName, clustersByName, visitor,
                new HashSet<>(), new HashSet<>(), new ArrayList<>());
    }

    @SuppressWarnings({ "java:S107", "java:S3776" }) // DFS traversal inherently needs this state threaded through recursion
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
        if (rd == null) {
            return true;
        }

        boolean firstVisit = inPath.add(routerName);

        currentPath.add(routerName);
        WalkContext ctx = new WalkContext(incomingRoute, sourceRouter, firstVisit, List.copyOf(currentPath));

        if (!visitor.enterRouter(rd, ctx)) {
            currentPath.removeLast();
            if (firstVisit) {
                inPath.remove(routerName);
            }
            return false;
        }

        if (!firstVisit) {
            // Revisit: already on the DFS path (cycle). Don't recurse.
            currentPath.removeLast();
            return true;
        }

        // Fresh node: recurse into routes.
        boolean result = true;
        for (var route : rd.routes()) {
            if (route.cluster() != null) {
                ClusterDefinition cd = clustersByName.get(route.cluster());
                if (cd != null) {
                    WalkContext clusterCtx = new WalkContext(route, rd, true, List.copyOf(currentPath));
                    if (!visitor.visitClusterDefinition(cd, clusterCtx)) {
                        result = false;
                    }
                }
            }
            if (route.router() != null && !traverseGraph(route.router(), route, rd, routersByName, clustersByName, visitor, visited, inPath, currentPath)) {
                result = false;
            }
            if (!result) {
                break;
            }
        }

        currentPath.removeLast();
        inPath.remove(routerName);
        visited.add(routerName);
        return result;
    }

}
