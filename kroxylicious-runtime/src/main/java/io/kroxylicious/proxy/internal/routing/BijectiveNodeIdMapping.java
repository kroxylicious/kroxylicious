/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.routing;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Bijective node ID mapping: {@code V = offset + N × t}, where
 * {@code V} is the virtual ID, {@code t} is the target ID,
 * {@code N} is the number of routes, and {@code offset} is
 * the route's zero-based index.
 */
public record BijectiveNodeIdMapping(List<String> routes,
                                     Map<String, Integer> offsets)
        implements NodeIdMapping {

    public BijectiveNodeIdMapping(List<String> routes) {
        this(List.copyOf(routes), buildOffsets(routes));
    }

    public BijectiveNodeIdMapping {
        if (routes.size() < 2) {
            throw new IllegalArgumentException("BijectiveNodeIdMapping requires at least 2 routes");
        }
    }

    @Override
    public int toVirtual(String route, int targetNodeId) {
        Integer offset = offsets.get(route);
        if (offset == null) {
            throw new IllegalArgumentException("Unknown route: " + route);
        }
        return offset + routes.size() * targetNodeId;
    }

    @Override
    public RouteAndNode fromVirtual(int virtualNodeId) {
        int n = routes.size();
        int offset = Math.floorMod(virtualNodeId, n);
        int targetNodeId = Math.floorDiv(virtualNodeId, n);
        return new RouteAndNode(routes.get(offset), targetNodeId);
    }

    private static Map<String, Integer> buildOffsets(List<String> routes) {
        var offsets = new HashMap<String, Integer>(routes.size());
        for (int i = 0; i < routes.size(); i++) {
            offsets.put(routes.get(i), i);
        }
        return Map.copyOf(offsets);
    }
}
