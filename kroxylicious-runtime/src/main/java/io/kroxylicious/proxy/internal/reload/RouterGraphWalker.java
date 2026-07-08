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

/**
 * Walks a router DAG and tests whether any node satisfies a caller-supplied predicate.
 * <p>
 * Each concern (changed router names, changed cluster definition names, …) is expressed
 * as a predicate rather than re-implementing traversal. This keeps graph walking in one
 * place as the number of hot-reload concern types grows.
 */
final class RouterGraphWalker {

    private RouterGraphWalker() {
    }

    /**
     * Returns {@code true} if any node reachable from {@code routerName} in the router DAG
     * satisfies either predicate.
     *
     * @param routerName          name of the entry-point router
     * @param routersByName       all router definitions indexed by name
     * @param routerNamePredicate test applied to each visited router name
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
