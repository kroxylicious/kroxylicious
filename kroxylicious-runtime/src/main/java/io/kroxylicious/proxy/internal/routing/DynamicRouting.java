/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.routing;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import io.kroxylicious.proxy.bootstrap.RouterChainFactory;
import io.kroxylicious.proxy.router.Router;

/**
 * Routing model for a virtual cluster that forwards to one or more upstream clusters via a named
 * router plugin. The {@link NodeIdMapping} is derived from the route descriptors at construction
 * time. The {@link RouterChainFactory} is owned by this instance and is closed when the owning
 * {@link io.kroxylicious.proxy.model.VirtualClusterModel} is closed.
 */
public record DynamicRouting(
                             String routerName,
                             Map<String, RouteDescriptor> routeDescriptors,
                             NodeIdMapping nodeIdMapping,
                             RouterChainFactory routerChainFactory)
        implements RoutingModel {

    /**
     * Convenience constructor: computes the {@link NodeIdMapping} from the supplied route descriptors.
     */
    public DynamicRouting(String routerName, Map<String, RouteDescriptor> routeDescriptors, RouterChainFactory routerChainFactory) {
        this(routerName, routeDescriptors, buildNodeIdMapping(routeDescriptors), routerChainFactory);
    }

    public DynamicRouting {
        Objects.requireNonNull(routerName, "routerName");
        Objects.requireNonNull(routeDescriptors, "routeDescriptors");
        Objects.requireNonNull(nodeIdMapping, "nodeIdMapping");
        Objects.requireNonNull(routerChainFactory, "routerChainFactory");
        routeDescriptors = Map.copyOf(routeDescriptors);
    }

    public Router createRouter(String clusterName) {
        return routerChainFactory.createRouter(routerName, clusterName);
    }

    @Override
    public void close() {
        routerChainFactory.close();
    }

    private static NodeIdMapping buildNodeIdMapping(Map<String, RouteDescriptor> routeDescriptors) {
        Objects.requireNonNull(routeDescriptors, "routeDescriptors");
        if (routeDescriptors.isEmpty()) {
            throw new IllegalArgumentException("DynamicRouting requires at least one route descriptor");
        }
        if (routeDescriptors.size() == 1) {
            return new IdentityNodeIdMapping(routeDescriptors.keySet().iterator().next());
        }
        var routeIds = new HashMap<String, Integer>(routeDescriptors.size());
        for (var entry : routeDescriptors.entrySet()) {
            routeIds.put(entry.getKey(), entry.getValue().id());
        }
        return new BijectiveNodeIdMapping(routeIds, routeIds.size());
    }
}
