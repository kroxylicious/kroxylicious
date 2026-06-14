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

import io.kroxylicious.proxy.config.PluginFactory;
import io.kroxylicious.proxy.config.PluginFactoryRegistry;
import io.kroxylicious.proxy.config.RouterDefinition;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;
import io.kroxylicious.proxy.router.Router;
import io.kroxylicious.proxy.router.RouterFactory;
import io.kroxylicious.proxy.router.RouterFactoryContext;
import io.kroxylicious.proxy.topology.TopologyService;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Abstracts the creation of router instances, hiding the configuration
 * required for instantiation at the point at which instances are created.
 */
public class RouterChainFactory implements AutoCloseable {

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

    private final Map<String, Wrapper> initialized;

    public RouterChainFactory(PluginFactoryRegistry pfr,
                              @Nullable List<RouterDefinition> routerDefinitions) {
        @SuppressWarnings({ "unchecked", "rawtypes" })
        Class<RouterFactory<? super Object, ? super Object>> type = (Class) RouterFactory.class;
        PluginFactory<RouterFactory<? super Object, ? super Object>> pluginFactory = pfr.pluginFactory(type);
        if (routerDefinitions == null || routerDefinitions.isEmpty()) {
            this.initialized = Map.of();
        }
        else {
            this.initialized = new LinkedHashMap<>(routerDefinitions.size());
            try {
                for (var rd : routerDefinitions) {
                    RouterFactoryContext context = new RouterFactoryContext() {
                        @Override
                        public String virtualClusterName() {
                            return "";
                        }

                        @Override
                        public String routerName() {
                            return rd.name();
                        }

                        @Override
                        public <P> P pluginInstance(Class<P> pluginClass, String implementationName) {
                            return pfr.pluginFactory(pluginClass).pluginInstance(implementationName);
                        }

                        @Override
                        public <P> Set<String> pluginImplementationNames(Class<P> pluginClass) {
                            return pfr.pluginFactory(pluginClass).registeredInstanceNames();
                        }

                        @Override
                        public Set<String> routeNames() {
                            return Set.of();
                        }

                        @Override
                        public TopologyService topologyService() {
                            throw new UnsupportedOperationException("TopologyService not available in this context");
                        }

                        @Override
                        public void allowSharedClusterTargets() {
                        }
                    };
                    RouterFactory<? super Object, ? super Object> factory = pluginFactory.pluginInstance(rd.type());
                    Class<?> configType = pluginFactory.configType(rd.type());
                    if (rd.config() == null || configType.isInstance(rd.config())) {
                        Wrapper wrapper = new Wrapper(context, rd, factory);
                        this.initialized.put(rd.name(), wrapper);
                    }
                    else {
                        throw new PluginConfigurationException(
                                "Router " + rd.name() + " accepts config of type "
                                        + configType.getName() + " but provided with config of type "
                                        + rd.config().getClass().getName());
                    }
                }
            }
            catch (Exception e) {
                close();
                throw e;
            }
        }
    }

    /**
     * Creates a new router instance for the given router name.
     *
     * @param routerName the name of the router definition
     * @param context the factory context for creating the router
     * @return the created router instance
     */
    public Router createRouter(String routerName,
                               RouterFactoryContext context) {
        Wrapper wrapper = initialized.get(routerName);
        if (wrapper == null) {
            throw new IllegalArgumentException("No router definition found for name: " + routerName);
        }
        return wrapper.create(context);
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
