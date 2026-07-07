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

import edu.umd.cs.findbugs.annotations.Nullable;

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
                             NodeIdMapping nodeIdMapping,
                             RouterChainFactory routerChainFactory,
                             Map<String, UpstreamClusterModel> routeClusterModels)
        implements RoutingModel {

    /**
     * Production constructor: computes the {@link NodeIdMapping} from the supplied route descriptors.
     */
    public DynamicRouting(String routerName, Map<String, RouteDescriptor> routeDescriptors,
                          RouterChainFactory routerChainFactory, Map<String, UpstreamClusterModel> routeClusterModels) {
        this(routerName, routeDescriptors, buildNodeIdMapping(routeDescriptors), routerChainFactory, routeClusterModels);
    }

    /**
     * Test-only constructor: uses an empty cluster model map.
     * Production code should supply fully-built {@link UpstreamClusterModel} instances.
     */
    @VisibleForTesting
    public DynamicRouting(String routerName, Map<String, RouteDescriptor> routeDescriptors, RouterChainFactory routerChainFactory) {
        this(routerName, routeDescriptors, buildNodeIdMapping(routeDescriptors), routerChainFactory, Map.of());
    }

    public DynamicRouting {
        Objects.requireNonNull(routerName, "routerName");
        Objects.requireNonNull(routeDescriptors, "routeDescriptors");
        Objects.requireNonNull(nodeIdMapping, "nodeIdMapping");
        Objects.requireNonNull(routerChainFactory, "routerChainFactory");
        Objects.requireNonNull(routeClusterModels, "routeClusterModels");
        routeDescriptors = Map.copyOf(routeDescriptors);
        routeClusterModels = Map.copyOf(routeClusterModels);
    }

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
    public @Nullable UpstreamClusterModel upstreamClusterFor(@Nullable String routeName) {
        if (routeName == null) {
            return null;
        }
        return routeClusterModels.get(routeName);
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
