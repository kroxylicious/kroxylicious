/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.config;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Validates that router definitions form a directed acyclic graph (DAG).
 */
class RouterGraphValidator {

    static final int MAX_SAFE_TARGET_NODE_ID = 100_000;

    private RouterGraphValidator() {
    }

    /**
     * Validates that the given router definitions form a DAG, and that all
     * target cluster and router references are resolvable.
     *
     * @param routerDefinitions the router definitions to validate
     * @param targetClusterNames the set of known target cluster names
     * @throws IllegalConfigurationException if the graph contains a cycle or a dangling reference
     */
    static void validate(List<RouterDefinition> routerDefinitions,
                         Set<String> targetClusterNames) {
        Map<String, RouterDefinition> routersByName = routerDefinitions.stream()
                .collect(Collectors.toMap(RouterDefinition::name, r -> r));

        validateReferences(routerDefinitions, routersByName, targetClusterNames);
        validateRouteIds(routerDefinitions);
        detectCycles(routerDefinitions, routersByName);
    }

    private static void validateReferences(List<RouterDefinition> routerDefinitions,
                                           Map<String, RouterDefinition> routersByName,
                                           Set<String> targetClusterNames) {
        for (var router : routerDefinitions) {
            for (var route : router.routes()) {
                if (route.cluster() != null && !targetClusterNames.contains(route.cluster())) {
                    throw new IllegalConfigurationException(
                            "Route '" + route.name() + "' in router '" + router.name()
                                    + "' references unknown cluster '" + route.cluster() + "'");
                }
                if (route.router() != null && !routersByName.containsKey(route.router())) {
                    throw new IllegalConfigurationException(
                            "Route '" + route.name() + "' in router '" + router.name()
                                    + "' references unknown router '" + route.router() + "'");
                }
            }
        }
    }

    private static void validateRouteIds(List<RouterDefinition> routerDefinitions) {
        for (var router : routerDefinitions) {
            int routeCount = router.routes().size();
            for (var route : router.routes()) {
                if (route.id() >= routeCount) {
                    throw new IllegalConfigurationException(
                            "Route '" + route.name() + "' in router '" + router.name()
                                    + "' has id " + route.id()
                                    + " which is outside the valid range [0, " + routeCount + ")");
                }
            }
            if (routeCount >= 2) {
                long maxVirtual = (long) routeCount * MAX_SAFE_TARGET_NODE_ID;
                if (maxVirtual > Integer.MAX_VALUE) {
                    throw new IllegalConfigurationException(
                            "Router '" + router.name() + "' has " + routeCount
                                    + " routes which risks integer overflow in virtual node ID mapping"
                                    + " for target node IDs up to " + MAX_SAFE_TARGET_NODE_ID);
                }
            }
        }
    }

    private static void detectCycles(List<RouterDefinition> routerDefinitions,
                                     Map<String, RouterDefinition> routersByName) {
        for (var rd : routerDefinitions) {
            RoutingGraphWalker.walkRouterGraph(rd.name(), routersByName, Map.of(),
                    CycleDetectionVisitor::new);
        }
    }

    private static final class CycleDetectionVisitor implements RoutingGraphVisitor<Void> {

        @Override
        public boolean enterRouter(RouterDefinition rd, WalkContext ctx) {
            if (!ctx.isFirstVisit()) {
                throw new IllegalConfigurationException(
                        "Router definitions contain a cycle: " + String.join(" -> ", ctx.path()));
            }
            return true;
        }

        @Override
        public Void result() {
            return null;
        }
    }
}
