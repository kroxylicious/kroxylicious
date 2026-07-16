/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.reload;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.function.Supplier;

import io.kroxylicious.proxy.config.ClusterDefinition;
import io.kroxylicious.proxy.config.RouterDefinition;
import io.kroxylicious.proxy.config.VirtualCluster;

/**
 * Walks the cluster graph rooted at a {@link VirtualCluster} (or a named router),
 * notifying a {@link ClusterGraphVisitor} at each node.
 * <p>
 * A virtual cluster always has a graph: either a trivial one (a single named cluster) or a
 * router DAG. Each concern can be expressed as a {@link ClusterGraphVisitor} rather than
 * re-implementing traversal.
 */
final class ClusterGraphWalker {

    private ClusterGraphWalker() {
    }

    /**
     * Walks the full cluster graph reachable from {@code vc} and returns the visitor's result.
     * <p>
     * A fresh visitor is obtained from {@code visitorFactory} at the start of each call, so
     * visitor state is never shared across walks.
     *
     * @param vc             the virtual cluster whose graph is walked
     * @param routersByName  all router definitions indexed by name
     * @param clustersByName all cluster definitions indexed by name; used to resolve cluster
     *                       names at route leaves into full {@link ClusterDefinition} objects
     * @param visitorFactory produces a new, ready-to-use visitor for this walk
     * @param <T>            the result type produced by the visitor
     * @return the value returned by {@link ClusterGraphVisitor#result()} after the walk
     */
    static <T> T walkClusterGraph(VirtualCluster vc,
                                  Map<String, RouterDefinition> routersByName,
                                  Map<String, ClusterDefinition> clustersByName,
                                  Supplier<ClusterGraphVisitor<T>> visitorFactory) {
        var visitor = visitorFactory.get();
        traverseClusterGraph(vc, routersByName, clustersByName, visitor);
        return visitor.result();
    }

    /**
     * Walks the router DAG reachable from {@code routerName} and returns the visitor's result.
     * <p>
     * A fresh visitor is obtained from {@code visitorFactory} at the start of each call, so
     * visitor state is never shared across walks.
     * <p>
     * Cycle-safe: each router name is visited at most once.
     *
     * @param routerName     name of the entry-point router
     * @param routersByName  all router definitions indexed by name
     * @param clustersByName all cluster definitions indexed by name; used to resolve cluster
     *                       names at route leaves into full {@link ClusterDefinition} objects
     * @param visitorFactory produces a new, ready-to-use visitor for this walk
     * @param <T>            the result type produced by the visitor
     * @return the value returned by {@link ClusterGraphVisitor#result()} after the walk
     */
    static <T> T walkRouterGraph(String routerName,
                                 Map<String, RouterDefinition> routersByName,
                                 Map<String, ClusterDefinition> clustersByName,
                                 Supplier<ClusterGraphVisitor<T>> visitorFactory) {
        var visitor = visitorFactory.get();
        traverseRouterGraph(routerName, routersByName, clustersByName, visitor);
        return visitor.result();
    }

    /**
     * Returns {@code true} if any node reachable from {@code vc} in its cluster graph
     * satisfies either predicate.
     * <p>
     * Handles the trivial single-cluster case ({@code namedTargetCluster}) and the router-DAG
     * case transparently, so callers do not need to branch on the VC's target type.
     *
     * @param vc                   the virtual cluster whose graph is walked
     * @param routersByName        all router definitions indexed by name
     * @param clustersByName       all cluster definitions indexed by name
     * @param routerNamePredicate  test applied to each visited router definition's name
     * @param clusterNamePredicate test applied to each cluster definition's name at a leaf
     */
    static boolean anyInClusterGraph(VirtualCluster vc,
                                     Map<String, RouterDefinition> routersByName,
                                     Map<String, ClusterDefinition> clustersByName,
                                     Predicate<String> routerNamePredicate,
                                     Predicate<String> clusterNamePredicate) {
        return walkClusterGraph(vc, routersByName, clustersByName,
                () -> new AnyMatchVisitor(routerNamePredicate, clusterNamePredicate));
    }

    /**
     * Returns {@code true} if any node reachable from {@code routerName} in the router DAG
     * given as {@code routersByName} satisfies either predicate.
     *
     * @param routerName           name of the entry-point router
     * @param routersByName        all router definitions indexed by name
     * @param clustersByName       all cluster definitions indexed by name
     * @param routerNamePredicate  test applied to each visited router definition's name
     * @param clusterNamePredicate test applied to each cluster definition's name at a leaf
     */
    static boolean anyInRouterGraph(String routerName,
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
                                                ClusterGraphVisitor<?> visitor) {
        if (!visitor.visitVirtualCluster(vc)) {
            return false;
        }
        String namedCluster = vc.namedTargetCluster();
        if (namedCluster != null) {
            return visitor.visitClusterName(clustersByName.get(namedCluster));
        }
        String router = vc.router();
        if (router != null) {
            return traverseRouterGraph(router, routersByName, clustersByName, visitor);
        }
        return true;
    }

    private static boolean traverseRouterGraph(String routerName,
                                               Map<String, RouterDefinition> routersByName,
                                               Map<String, ClusterDefinition> clustersByName,
                                               ClusterGraphVisitor<?> visitor) {
        return traverseGraph(routerName, routersByName, clustersByName, visitor, new HashSet<>());
    }

    private static boolean traverseGraph(String routerName,
                                         Map<String, RouterDefinition> routersByName,
                                         Map<String, ClusterDefinition> clustersByName,
                                         ClusterGraphVisitor<?> visitor,
                                         Set<String> visited) {
        if (!visited.add(routerName)) {
            return true;
        }
        RouterDefinition rd = routersByName.get(routerName);
        if (!visitor.visitRouter(rd)) {
            return false;
        }
        if (rd == null) {
            return true;
        }
        for (var route : rd.routes()) {
            if (route.cluster() != null && !visitor.visitClusterName(clustersByName.get(route.cluster()))) {
                return false;
            }
            if (route.router() != null && !traverseGraph(route.router(), routersByName, clustersByName, visitor, visited)) {
                return false;
            }
        }
        return true;
    }

    private static final class AnyMatchVisitor implements ClusterGraphVisitor<Boolean> {

        private final Predicate<String> routerNamePredicate;
        private final Predicate<String> clusterNamePredicate;
        private boolean found;

        AnyMatchVisitor(Predicate<String> routerNamePredicate, Predicate<String> clusterNamePredicate) {
            this.routerNamePredicate = routerNamePredicate;
            this.clusterNamePredicate = clusterNamePredicate;
        }

        @Override
        public boolean visitRouter(RouterDefinition rd) {
            if (rd != null && routerNamePredicate.test(rd.name())) {
                found = true;
                return false;
            }
            return true;
        }

        @Override
        public boolean visitClusterName(ClusterDefinition cd) {
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
