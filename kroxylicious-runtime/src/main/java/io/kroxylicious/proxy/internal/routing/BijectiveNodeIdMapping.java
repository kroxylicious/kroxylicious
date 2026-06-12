/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.routing;

import java.util.Map;

/**
 * Bijective node ID mapping: {@code V = id + S × t}, where
 * {@code V} is the virtual ID, {@code t} is the target node ID,
 * {@code S} is the total number of routes, and {@code id} is
 * the route's configured identifier.
 */
public record BijectiveNodeIdMapping(Map<String, Integer> routeIds,
                                     int totalRoutes)
        implements NodeIdMapping {

    public BijectiveNodeIdMapping {
        routeIds = Map.copyOf(routeIds);
        if (totalRoutes < 2) {
            throw new IllegalArgumentException("BijectiveNodeIdMapping requires at least 2 routes, got " + totalRoutes);
        }
        for (var entry : routeIds.entrySet()) {
            int id = entry.getValue();
            if (id < 0 || id >= totalRoutes) {
                throw new IllegalArgumentException(
                        "Route '" + entry.getKey() + "' has id " + id
                                + " which is outside the valid range [0, " + totalRoutes + ")");
            }
        }
    }

    @Override
    public int toVirtual(String route, int targetNodeId) {
        Integer id = routeIds.get(route);
        if (id == null) {
            throw new IllegalArgumentException("Unknown route: " + route);
        }
        return Math.addExact(id, Math.multiplyExact(totalRoutes, targetNodeId));
    }

    @Override
    public int fromVirtual(String route, int virtualNodeId) {
        return Math.floorDiv(virtualNodeId, totalRoutes);
    }

    @Override
    public RouteAndNode fromVirtual(int virtualNodeId) {
        int id = Math.floorMod(virtualNodeId, totalRoutes);
        int targetNodeId = Math.floorDiv(virtualNodeId, totalRoutes);
        String route = routeIds.entrySet().stream()
                .filter(e -> e.getValue() == id)
                .map(Map.Entry::getKey)
                .findFirst()
                .orElseThrow(() -> new IllegalStateException(
                        "No route with id " + id + " (virtual node ID " + virtualNodeId + ")"));
        return new RouteAndNode(route, targetNodeId);
    }
}
