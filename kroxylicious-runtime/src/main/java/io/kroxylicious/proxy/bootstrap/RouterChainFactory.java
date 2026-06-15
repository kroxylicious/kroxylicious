/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.bootstrap;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import io.kroxylicious.proxy.config.PluginFactory;
import io.kroxylicious.proxy.config.PluginFactoryRegistry;
import io.kroxylicious.proxy.config.RouteDefinition;
import io.kroxylicious.proxy.config.RouterDefinition;
import io.kroxylicious.proxy.config.VirtualCluster;
import io.kroxylicious.proxy.internal.routing.TopologyCache;
import io.kroxylicious.proxy.internal.routing.TopologyServiceImpl;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;
import io.kroxylicious.proxy.router.Router;
import io.kroxylicious.proxy.router.RouterFactory;
import io.kroxylicious.proxy.router.RouterFactoryContext;
import io.kroxylicious.proxy.router.TopologyService;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Abstracts the creation of router instances, hiding the configuration
 * required for instantiation at the point at which instances are created.
 *
 * <p>Each virtual cluster that references a router gets its own
 * initialisation of that router's factory, so shared state (e.g.
 * caches, metrics) is per-virtual-cluster.</p>
 */
public class RouterChainFactory implements AutoCloseable {

    record VcRouter(String virtualClusterName, String routerName) {}

    private static final class Wrapper {

        private final RouterFactory<? super Object, ? super Object> routerFactory;
        private final RouterDefinition routerDefinition;
        private final Object initResult;
        private final AtomicBoolean closed = new AtomicBoolean(false);

        private Wrapper(RouterFactoryContext context,
                        RouterDefinition routerDefinition,
                        RouterFactory<? super Object, ? super Object> routerFactory) {
            this.routerFactory = routerFactory;
            this.routerDefinition = routerDefinition;
            Object config = routerDefinition.config();
            try {
                initResult = routerFactory.initialize(context, config);
            }
            catch (Exception e) {
                throw new PluginConfigurationException(
                        "Exception initializing router factory " + routerDefinition.name()
                                + " with config " + config + ": " + e.getMessage(),
                        e);
            }
        }

        private Router create(RouterFactoryContext context) {
            if (closed.get()) {
                throw new IllegalStateException("Router factory " + routerDefinition.name() + " is closed");
            }
            try {
                return routerFactory.createRouter(context, initResult);
            }
            catch (Exception e) {
                throw new PluginConfigurationException(
                        "Exception instantiating router " + routerDefinition.name()
                                + " using factory " + routerFactory,
                        e);
            }
        }

        private void close() {
            if (!this.closed.getAndSet(true)) {
                routerFactory.close(initResult);
            }
        }
    }

    private final Map<VcRouter, Wrapper> initialized;
    private final Map<VcRouter, TopologyServiceImpl> topologyServices = new LinkedHashMap<>();
    private final Map<VcRouter, java.util.concurrent.ConcurrentHashMap<Integer, io.kroxylicious.proxy.service.HostPort>> sharedNodeAddressMaps = new LinkedHashMap<>();
    private final PluginFactoryRegistry pfr;

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public RouterChainFactory(PluginFactoryRegistry pfr,
                              List<VirtualCluster> virtualClusters,
                              @Nullable List<RouterDefinition> routerDefinitions) {
        this.pfr = pfr;
        Class<RouterFactory<? super Object, ? super Object>> type = (Class) RouterFactory.class;
        PluginFactory<RouterFactory<? super Object, ? super Object>> pluginFactory = pfr.pluginFactory(type);

        if (routerDefinitions == null || routerDefinitions.isEmpty()) {
            this.initialized = Map.of();
        }
        else {
            Map<String, RouterDefinition> routersByName = routerDefinitions.stream()
                    .collect(Collectors.toMap(RouterDefinition::name, r -> r));
            this.initialized = new LinkedHashMap<>();
            try {
                for (var vc : virtualClusters) {
                    if (vc.router() != null) {
                        initializeRouterGraph(vc.name(), vc.router(), routersByName, pluginFactory);
                    }
                }
            }
            catch (Exception e) {
                close();
                throw e;
            }
        }
    }

    private void initializeRouterGraph(String vcName,
                                       String routerName,
                                       Map<String, RouterDefinition> routersByName,
                                       PluginFactory<RouterFactory<? super Object, ? super Object>> pluginFactory) {
        var key = new VcRouter(vcName, routerName);
        if (initialized.containsKey(key)) {
            return;
        }
        RouterDefinition rd = routersByName.get(routerName);
        if (rd == null) {
            return;
        }
        RouterFactory<? super Object, ? super Object> factory = pluginFactory.pluginInstance(rd.type());
        Class<?> configType = pluginFactory.configType(rd.type());
        if (rd.config() != null && !configType.isInstance(rd.config())) {
            throw new PluginConfigurationException(
                    "Router " + rd.name() + " accepts config of type "
                            + configType.getName() + " but provided with config of type "
                            + rd.config().getClass().getName());
        }
        RouterFactoryContext context = createContext(vcName, rd);
        Wrapper wrapper = new Wrapper(context, rd, factory);
        initialized.put(key, wrapper);

        for (RouteDefinition route : rd.routes()) {
            if (route.router() != null) {
                initializeRouterGraph(vcName, route.router(), routersByName, pluginFactory);
            }
        }
    }

    /**
     * Creates a new router instance for the given router name and virtual cluster.
     *
     * @param routerName the name of the router definition
     * @param virtualClusterName the name of the virtual cluster
     * @return the created router instance
     */
    public Router createRouter(String routerName,
                               String virtualClusterName) {
        var key = new VcRouter(virtualClusterName, routerName);
        Wrapper wrapper = initialized.get(key);
        if (wrapper == null) {
            throw new IllegalArgumentException(
                    "No router definition found for name: " + routerName
                            + " in virtual cluster: " + virtualClusterName);
        }
        RouterFactoryContext context = createContext(virtualClusterName, wrapper.routerDefinition);
        return wrapper.create(context);
    }

    private RouterFactoryContext createContext(String vcName, RouterDefinition rd) {
        Set<String> routeNames = rd.routes().stream()
                .map(RouteDefinition::name)
                .collect(Collectors.toUnmodifiableSet());
        return new RouterFactoryContext() {
            @Override
            public String virtualClusterName() {
                return vcName;
            }

            @Override
            public String routerName() {
                return rd.name();
            }

            @Override
            public Set<String> routeNames() {
                return routeNames;
            }

            @Override
            public <P> P pluginInstance(Class<P> pluginClass,
                                        String implementationName) {
                return pfr.pluginFactory(pluginClass).pluginInstance(implementationName);
            }

            @Override
            public <P> Set<String> pluginImplementationNames(Class<P> pluginClass) {
                return pfr.pluginFactory(pluginClass).registeredInstanceNames();
            }

            @Override
            public TopologyService topologyService() {
                return topologyServices.computeIfAbsent(
                        new VcRouter(vcName, rd.name()),
                        k -> new TopologyServiceImpl(new TopologyCache()));
            }
        };
    }

    /**
     * Returns the {@link TopologyServiceImpl} for the given router level,
     * or null if no router at that level opted in to topology caching.
     */
    @Nullable
    public TopologyServiceImpl topologyServiceFor(String routerName, String virtualClusterName) {
        return topologyServices.get(new VcRouter(virtualClusterName, routerName));
    }

    /**
     * Returns the shared node address map for the given router level.
     * The map is created on first access and shared across all connections.
     * Thread-safe via {@link java.util.concurrent.ConcurrentHashMap}.
     */
    public java.util.concurrent.ConcurrentHashMap<Integer, io.kroxylicious.proxy.service.HostPort> sharedNodeAddressesFor(
                                                                                                                          String routerName, String virtualClusterName) {
        return sharedNodeAddressMaps.computeIfAbsent(
                new VcRouter(virtualClusterName, routerName),
                k -> new java.util.concurrent.ConcurrentHashMap<>());
    }

    @Override
    public void close() {
        RuntimeException firstThrown = null;
        var list = new ArrayList<>(initialized.values());
        for (int i = list.size() - 1; i >= 0; i--) {
            Wrapper wrapper = list.get(i);
            try {
                wrapper.close();
            }
            catch (RuntimeException e) {
                if (firstThrown == null) {
                    firstThrown = e;
                }
                else {
                    firstThrown.addSuppressed(e);
                }
            }
        }
        if (firstThrown != null) {
            throw firstThrown;
        }
    }
}
