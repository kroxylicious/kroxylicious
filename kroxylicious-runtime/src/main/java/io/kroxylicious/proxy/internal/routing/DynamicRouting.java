/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.routing;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import io.netty.handler.ssl.SslContext;

import io.kroxylicious.proxy.bootstrap.TlsCredentialSupplierManager;
import io.kroxylicious.proxy.config.TargetCluster;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Routing model for a virtual cluster that forwards to one or more upstream clusters via a named
 * router plugin. The {@link NodeIdMapping} is derived from the route descriptors at construction
 * time.
 * <p>
 * Owns per-route {@link #routeSslContexts()} and {@link #routeTlsManagers()}, which are populated
 * from route descriptors during {@code VirtualClusterModel} construction. Empty maps are used when
 * no TLS resources have been resolved (e.g. in test contexts without a {@code PluginFactoryRegistry}).
 */
@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public record DynamicRouting(
                             String routerName,
                             Map<String, RouteDescriptor> routeDescriptors,
                             NodeIdMapping nodeIdMapping,
                             Map<String, Optional<SslContext>> routeSslContexts,
                             Map<String, TlsCredentialSupplierManager> routeTlsManagers)
        implements RoutingModel {

    /**
     * Convenience constructor: computes the {@link NodeIdMapping} from the supplied route descriptors
     * and uses empty maps for TLS resources (resolved later by {@code VirtualClusterModel}).
     */
    public DynamicRouting(String routerName, Map<String, RouteDescriptor> routeDescriptors) {
        this(routerName, routeDescriptors, buildNodeIdMapping(routeDescriptors), Map.of(), Map.of());
    }

    public DynamicRouting {
        Objects.requireNonNull(routerName, "routerName");
        Objects.requireNonNull(routeDescriptors, "routeDescriptors");
        Objects.requireNonNull(nodeIdMapping, "nodeIdMapping");
        Objects.requireNonNull(routeSslContexts, "routeSslContexts");
        Objects.requireNonNull(routeTlsManagers, "routeTlsManagers");
        routeDescriptors = Map.copyOf(routeDescriptors);
        routeSslContexts = Map.copyOf(routeSslContexts);
        routeTlsManagers = Map.copyOf(routeTlsManagers);
    }

    @Override
    public @Nullable TargetCluster targetClusterFor(@Nullable String routeName) {
        if (routeName == null) {
            return null;
        }
        RouteDescriptor descriptor = routeDescriptors.get(routeName);
        return descriptor != null ? descriptor.targetCluster() : null;
    }

    @Override
    public Optional<SslContext> upstreamSslContextFor(@Nullable String routeName) {
        if (routeName == null) {
            return Optional.empty();
        }
        return routeSslContexts.getOrDefault(routeName, Optional.empty());
    }

    @Override
    public TlsCredentialSupplierManager tlsManagerFor(@Nullable String routeName) {
        if (routeName == null) {
            return TlsCredentialSupplierManager.unconfigured();
        }
        return routeTlsManagers.getOrDefault(routeName, TlsCredentialSupplierManager.unconfigured());
    }

    @Override
    public void close() {
        RuntimeException firstFailure = null;
        for (TlsCredentialSupplierManager manager : routeTlsManagers.values()) {
            try {
                manager.close();
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
