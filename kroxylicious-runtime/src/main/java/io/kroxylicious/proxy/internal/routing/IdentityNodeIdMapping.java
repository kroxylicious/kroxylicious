/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.routing;

/**
 * Identity node ID mapping for single-route configurations.
 * Virtual IDs equal target IDs. Negative node IDs (Kafka protocol sentinels)
 * are returned unchanged by all methods, satisfying the {@link NodeIdMapping} contract.
 */
public record IdentityNodeIdMapping(String routeName) implements NodeIdMapping {

    @Override
    public int toVirtual(String route, int targetNodeId) {
        return targetNodeId;
    }

    @Override
    public int fromVirtual(String route, int virtualNodeId) {
        return virtualNodeId;
    }

    @Override
    public RouteAndNode fromVirtual(int virtualNodeId) {
        return new RouteAndNode(routeName, virtualNodeId);
    }
}
