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
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import io.kroxylicious.proxy.config.PluginFactory;
import io.kroxylicious.proxy.config.PluginFactoryRegistry;
import io.kroxylicious.proxy.config.RouteDefinition;
import io.kroxylicious.proxy.config.RouterDefinition;
import io.kroxylicious.proxy.config.VirtualCluster;
import io.kroxylicious.proxy.internal.topology.RequestSender;
import io.kroxylicious.proxy.internal.topology.TopologyCache;
import io.kroxylicious.proxy.internal.topology.TopologyCacheHolder;
import io.kroxylicious.proxy.internal.topology.TopologyServiceImpl;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;
import io.kroxylicious.proxy.router.Router;
import io.kroxylicious.proxy.router.RouterFactory;
import io.kroxylicious.proxy.router.RouterFactoryContext;
import io.kroxylicious.proxy.topology.TopologyService;

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
        private final String routerName;
        private final RouterFactoryContext context;
        private final Object initResult;
        private final TopologyCacheHolder cacheHolder;
        private final AtomicBoolean closed = new AtomicBoolean(false);

        private Wrapper(RouterFactoryContext context,
                        RouterDefinition routerDefinition,
                        RouterFactory<? super Object, ? super Object> routerFactory,
                        TopologyCacheHolder cacheHolder) {
            this.routerFactory = routerFactory;
            this.routerName = routerDefinition.name();
            this.context = context;
            this.cacheHolder = cacheHolder;
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

        /**
         * Creates a per-connection router instance. Builds a per-call {@link RouterFactoryContext}
         * that delegates to the shared {@link #context} for everything except
         * {@link RouterFactoryContext#topologyService()}, which it binds to the given
         * {@code sender} - this avoids a shared mutable "current sender" field on {@code Wrapper},
         * which would race since different connections' event-loop threads can call this method
         * concurrently.
         */
        private Router create(RequestSender sender) {
            if (closed.get()) {
                throw new IllegalStateException("Router factory " + routerName + " is closed");
            }
            RouterFactoryContext perConnectionContext = new RouterFactoryContext() {
                @Override
                public String virtualClusterName() {
                    return context.virtualClusterName();
                }

                @Override
                public String routerName() {
                    return context.routerName();
                }

                @Override
                public <P> P pluginInstance(Class<P> pluginClass, String implementationName) {
                    return context.pluginInstance(pluginClass, implementationName);
                }

                @Override
                public Set<String> routeNames() {
                    return context.routeNames();
                }

                @Override
                public <P> Set<String> pluginImplementationNames(Class<P> pluginClass) {
                    return context.pluginImplementationNames(pluginClass);
                }

                @Override
                public TopologyService topologyService() {
                    return new TopologyServiceImpl(cacheHolder.getOrCreate(), sender);
                }

                @Override
                public void allowSharedClusterTargets() {
                    context.allowSharedClusterTargets();
                }
            };
            try {
                return routerFactory.createRouter(perConnectionContext, initResult);
            }
            catch (Exception e) {
                throw new PluginConfigurationException(
                        "Exception instantiating router " + routerName
                                + " using factory " + routerFactory,
                        e);
            }
        }

        /**
         * Returns the shared {@link TopologyCache} for this router level, if one has been created
         * (i.e. some connection's router has called {@link RouterFactoryContext#topologyService()}).
         */
        private Optional<TopologyCache> existingTopologyCache() {
            return Optional.ofNullable(cacheHolder.getIfPresent());
        }

        private void close() {
            if (!this.closed.getAndSet(true)) {
                routerFactory.close(initResult);
            }
        }
    }

    private final Map<VcRouter, Wrapper> initialized;
    private final PluginFactoryRegistry pfr;

    /**
     * Creates a {@link RouterChainFactory} for a single virtual cluster. The factory
     * initialises only the routers reachable from {@code vc}'s router graph.
     *
     * @param pfr the plugin factory registry
     * @param vc the virtual cluster whose router graph to initialise
     * @param routersByName all router definitions by name (the graph may reference any of them)
     * @return a new factory whose lifetime should match that of the virtual cluster
     */
    public static RouterChainFactory forVirtualCluster(PluginFactoryRegistry pfr,
                                                       VirtualCluster vc,
                                                       @Nullable Map<String, RouterDefinition> routersByName) {
        List<RouterDefinition> defs = routersByName == null ? null : new ArrayList<>(routersByName.values());
        return new RouterChainFactory(pfr, List.of(vc), defs);
    }

    /**
     * Creates a {@link RouterChainFactory} covering the given virtual clusters, initialising the
     * router graph reachable from each cluster's entry-point router. Initialisation failures
     * close any already-initialised router factories before propagating.
     *
     * @param pfr the plugin factory registry
     * @param virtualClusters the virtual clusters whose router graphs to initialise
     * @param routerDefinitions all router definitions; may be null or empty when no routers are configured
     */
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
        var routeNames = rd.routes().stream()
                .map(RouteDefinition::name)
                .collect(Collectors.toUnmodifiableSet());
        var cacheHolder = new TopologyCacheHolder();
        RouterFactoryContext context = createContext(vcName, routerName, routeNames, cacheHolder);
        Wrapper wrapper = new Wrapper(context, rd, factory, cacheHolder);
        initialized.put(key, wrapper);

        for (RouteDefinition route : rd.routes()) {
            if (route.router() != null) {
                initializeRouterGraph(vcName, route.router(), routersByName, pluginFactory);
            }
        }
    }

    /**
     * Creates a new router instance for the given router name and virtual cluster, whose
     * {@link RouterFactoryContext#topologyService()} always throws if used for discovery.
     * Prefer {@link #createRouter(String, String, RequestSender)} in production code; this
     * overload exists for callers (and tests) that don't have a {@link RequestSender} to hand.
     *
     * @param routerName the name of the router definition
     * @param virtualClusterName the name of the virtual cluster
     * @return the created router instance
     */
    public Router createRouter(String routerName,
                               String virtualClusterName) {
        return createRouter(routerName, virtualClusterName, RequestSender.unavailable());
    }

    /**
     * Creates a new router instance for the given router name and virtual cluster, whose
     * {@link RouterFactoryContext#topologyService()} (if called during
     * {@link RouterFactory#createRouter}) uses the given {@code sender} to send discovery
     * requests on this connection.
     *
     * @param routerName the name of the router definition
     * @param virtualClusterName the name of the virtual cluster
     * @param sender the request-sending capability to bind to this connection's topology service
     * @return the created router instance
     */
    public Router createRouter(String routerName,
                               String virtualClusterName,
                               RequestSender sender) {
        var key = new VcRouter(virtualClusterName, routerName);
        Wrapper wrapper = initialized.get(key);
        if (wrapper == null) {
            throw new IllegalArgumentException(
                    "No router definition found for name: " + routerName
                            + " in virtual cluster: " + virtualClusterName);
        }
        return wrapper.create(sender);
    }

    /**
     * Returns the shared {@link TopologyCache} for the given router level, if one has been
     * created (i.e. some connection's router has called
     * {@link RouterFactoryContext#topologyService()}). Used by the routing runtime to activate
     * cache population on a connection's dispatcher immediately after
     * {@link #createRouter(String, String, RequestSender)} returns.
     *
     * @param routerName the name of the router definition
     * @param virtualClusterName the name of the virtual cluster
     * @return the shared topology cache, or empty if none has been created yet
     */
    public Optional<TopologyCache> existingTopologyCache(String routerName, String virtualClusterName) {
        var key = new VcRouter(virtualClusterName, routerName);
        Wrapper wrapper = initialized.get(key);
        return wrapper == null ? Optional.empty() : wrapper.existingTopologyCache();
    }

    private RouterFactoryContext createContext(String vcName, String routerName, Set<String> routeNames, TopologyCacheHolder cacheHolder) {
        return new RouterFactoryContext() {
            @Override
            public String virtualClusterName() {
                return vcName;
            }

            @Override
            public String routerName() {
                return routerName;
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
            public Set<String> routeNames() {
                return routeNames;
            }

            @Override
            public TopologyService topologyService() {
                // This is the shared, RouterFactory#initialize-time context (createRouter builds
                // its own per-call context in Wrapper.create) - per topologyService()'s contract,
                // the instance returned here must not be stored or used for discovery, only to
                // trigger cache creation as an opt-in side effect.
                return new TopologyServiceImpl(cacheHolder.getOrCreate(), RequestSender.unavailable());
            }

            @Override
            public void allowSharedClusterTargets() {
                // no-op: shared-cluster-target validation is not yet enforced by the runtime
            }
        };
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
