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

import io.kroxylicious.proxy.config.RouterDefinition;
import io.kroxylicious.proxy.config.VirtualCluster;

/**
 * Tests whether any node in a cluster graph satisfies a caller-supplied predicate.
 * <p>
 * A virtual cluster always has a graph: either a trivial one (a single named cluster) or a
 * router DAG. Each concern (changed router names, changed cluster definition names, …) is
 * expressed as a predicate rather than re-implementing traversal.
 */
final class ClusterGraphTester {

    private ClusterGraphTester() {
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
     * @param routerNamePredicate  test applied to each visited router name
     * @param clusterNamePredicate test applied to each named cluster at a leaf
     */
    static boolean anyInClusterGraph(VirtualCluster vc,
                                     Map<String, RouterDefinition> routersByName,
                                     Predicate<String> routerNamePredicate,
                                     Predicate<String> clusterNamePredicate) {
        String namedCluster = vc.namedTargetCluster();
        if (namedCluster != null) {
            return clusterNamePredicate.test(namedCluster);
        }
        String router = vc.router();
        if (router != null) {
            return anyInRouterGraph(router, routersByName, routerNamePredicate, clusterNamePredicate);
        }
        return false;
    }

    /**
     * Returns {@code true} if any node reachable from {@code routerName} in the router DAG
     * given as {@code routersByName} satisfies either predicate.
     *
     * @param routerName           name of the entry-point router
     * @param routersByName        all router definitions indexed by name
     * @param routerNamePredicate  test applied to each visited router name
     * @param clusterNamePredicate test applied to each cluster name found at a route leaf
     */
    static boolean anyInRouterGraph(String routerName,
                                    Map<String, RouterDefinition> routersByName,
                                    Predicate<String> routerNamePredicate,
                                    Predicate<String> clusterNamePredicate) {
        return anyInGraph(routerName, routersByName, routerNamePredicate, clusterNamePredicate, new HashSet<>());
    }

    private static boolean anyInGraph(String routerName,
                                      Map<String, RouterDefinition> routersByName,
                                      Predicate<String> routerNamePredicate,
                                      Predicate<String> clusterNamePredicate,
                                      Set<String> visited) {
        if (!visited.add(routerName)) {
            return false;
        }
        if (routerNamePredicate.test(routerName)) {
            return true;
        }
        RouterDefinition rd = routersByName.get(routerName);
        if (rd == null) {
            return false;
        }
        for (var route : rd.routes()) {
            if (route.cluster() != null && clusterNamePredicate.test(route.cluster())) {
                return true;
            }
            if (route.router() != null && anyInGraph(route.router(), routersByName, routerNamePredicate, clusterNamePredicate, visited)) {
                return true;
            }
        }
        return false;
    }
}
