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
import io.kroxylicious.proxy.tag.VisibleForTesting;

/**
 * Routing model for a virtual cluster that forwards to one or more upstream clusters via a named
 * router plugin. The {@link NodeIdMapping} is derived from the route descriptors at construction
 * time. The {@link RouterChainFactory} is owned by this instance and is closed when the owning
 * {@link io.kroxylicious.proxy.model.VirtualClusterModel} is closed.
 * <p>
 * Owns per-route {@link UpstreamClusterModel} instances in {@link #routeClusterModels()}, which are
 * populated from route descriptors during {@code VirtualClusterModel} construction. An empty map is
 * used when no TLS resources have been resolved (e.g. in test contexts without a
 * {@code PluginFactoryRegistry}).
 */
public record DynamicRouting(
                             String routerName,
                             Map<String, RouteDescriptor> routeDescriptors,
                             Map<String, RouteDescriptor> allRouteDescriptors,
                             NodeIdMapping nodeIdMapping,
                             RouterChainFactory routerChainFactory,
                             Map<String, UpstreamClusterModel> routeClusterModels)
        implements RoutingModel {

    /**
     * Production constructor: computes the {@link NodeIdMapping} from the top-level route descriptors.
     *
     * @param routerName the name of the top-level router
     * @param routeDescriptors the top-level router's route descriptors (local names)
     * @param allRouteDescriptors all route descriptors including nested routers (qualified names: {@code routerName/routeName})
     * @param routerChainFactory the router chain factory
     * @param routeClusterModels upstream cluster models for all cluster-targeting routes (qualified names)
     */
    public DynamicRouting(String routerName, Map<String, RouteDescriptor> routeDescriptors,
                          Map<String, RouteDescriptor> allRouteDescriptors,
                          RouterChainFactory routerChainFactory, Map<String, UpstreamClusterModel> routeClusterModels) {
        this(routerName, routeDescriptors, allRouteDescriptors, buildNodeIdMapping(routeDescriptors), routerChainFactory, routeClusterModels);
    }

    /**
     * Test-only constructor: uses an empty cluster model map. {@code allRouteDescriptors}
     * defaults to the provided {@code routeDescriptors} (i.e. no nested routes).
     * Production code should supply fully-built {@link UpstreamClusterModel} instances.
     */
    @VisibleForTesting
    public DynamicRouting(String routerName, Map<String, RouteDescriptor> routeDescriptors, RouterChainFactory routerChainFactory) {
        this(routerName, routeDescriptors, routeDescriptors, buildNodeIdMapping(routeDescriptors), routerChainFactory, Map.of());
    }

    /**
     * Validates all components and takes defensive copies of the maps.
     */
    public DynamicRouting {
        Objects.requireNonNull(routerName, "routerName");
        Objects.requireNonNull(routeDescriptors, "routeDescriptors");
        Objects.requireNonNull(allRouteDescriptors, "allRouteDescriptors");
        Objects.requireNonNull(nodeIdMapping, "nodeIdMapping");
        Objects.requireNonNull(routerChainFactory, "routerChainFactory");
        Objects.requireNonNull(routeClusterModels, "routeClusterModels");
        routeDescriptors = Map.copyOf(routeDescriptors);
        allRouteDescriptors = Map.copyOf(allRouteDescriptors);
        routeClusterModels = Map.copyOf(routeClusterModels);
    }

    /**
     * Creates a router instance for the given virtual cluster.
     *
     * @param clusterName the name of the virtual cluster
     * @return the router
     */
    public Router createRouter(String clusterName) {
        return routerChainFactory.createRouter(routerName, clusterName);
    }

    @Override
    public void close() {
        RuntimeException firstFailure = null;
        try {
            routerChainFactory.close();
        }
        catch (RuntimeException e) {
            firstFailure = e;
        }
        for (UpstreamClusterModel model : routeClusterModels.values()) {
            try {
                model.close();
            }
            catch (RuntimeException e) {
                if (firstFailure == null) {
                    firstFailure = e;
                }
                else {
                    firstFailure.addSuppressed(e);
                }
            }
        }
        if (firstFailure != null) {
            throw firstFailure;
        }
    }

    @Override
    public UpstreamClusterModel upstreamClusterFor(String routeName) {
        UpstreamClusterModel upstreamClusterModel = routeClusterModels.get(routeName);
        if (upstreamClusterModel == null) {
            RouteDescriptor routeDescriptor = routeDescriptors.get(routeName);
            if (routeDescriptor == null) {
                throw new NoUpstreamClusterForRouteException("route " + routeName + " does not exist");
            }
            else if (!routeDescriptor.targetsCluster()) {
                throw new NoUpstreamClusterForRouteException("route " + routeName + " does not target a cluster, but targets router " + routeDescriptor.routerName());
            }
            else {
                throw new NoUpstreamClusterForRouteException("route " + routeName + " has no upstream cluster");
            }
        }
        return upstreamClusterModel;
    }

    private static NodeIdMapping buildNodeIdMapping(Map<String, RouteDescriptor> routeDescriptors) {
        Objects.requireNonNull(routeDescriptors, "routeDescriptors");
        if (routeDescriptors.isEmpty()) {
            throw new IllegalArgumentException("DynamicRouting requires at least one route descriptor");
        }
        if (routeDescriptors.size() == 1) {
            return new IdentityNodeIdMapping(routeDescriptors.keySet().iterator().next());
        }
        var routeIds = HashMap.<String, Integer> newHashMap(routeDescriptors.size());
        for (var entry : routeDescriptors.entrySet()) {
            routeIds.put(entry.getKey(), entry.getValue().id());
        }
        return new BijectiveNodeIdMapping(routeIds, routeIds.size());
    }
}
