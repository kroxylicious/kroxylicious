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
import java.util.concurrent.atomic.AtomicBoolean;

import io.kroxylicious.proxy.config.NamedFilterDefinition;
import io.kroxylicious.proxy.config.PluginFactory;
import io.kroxylicious.proxy.config.PluginFactoryRegistry;
import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.filter.FilterAndInvoker;
import io.kroxylicious.proxy.filter.FilterDispatchExecutor;
import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.filter.FilterOptions;
import io.kroxylicious.proxy.filter.TargetMessageClass;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Abstracts the creation of a chain of filter instances, hiding the configuration
 * required for instantiation at the point at which instances are created.
 * New instances are created during initialization of a downstream channel.
 */
public class FilterChainFactory implements AutoCloseable {

    /**
     * Manages the lifesystem of a filter instance, initializing it on construction and closing it in {@link #close()}
     */
    private static final class Wrapper {

        private final FilterFactory<? super Object, ? super Object> filterFactory;
        private final NamedFilterDefinition filterDefinition;
        private final Object initResult;
        private final AtomicBoolean closed = new AtomicBoolean(false);

        private Wrapper(FilterFactoryContext context,
                        NamedFilterDefinition filterDefinition,
                        FilterFactory<? super Object, ? super Object> filterFactory) {
            this.filterFactory = filterFactory;
            this.filterDefinition = filterDefinition;
            Object config = filterDefinition.config();
            try {
                initResult = filterFactory.initialize(context, config);
            }
            catch (Exception e) {
                throw new PluginConfigurationException(
                        "Exception initializing filter factory " + filterDefinition.name() + " with config " + config + ": " + e.getMessage(), e);
            }
        }

        public Filter create(FilterFactoryContext context) {
            if (closed.get()) {
                throw new IllegalStateException("Filter factory " + filterDefinition.name() + " is closed");
            }
            try {
                return filterFactory.createFilter(context, initResult);
            }
            catch (Exception e) {
                throw new PluginConfigurationException("Exception instantiating filter " + filterDefinition.name() + " using factory " + filterFactory, e);
            }
        }

        public void close() {
            if (!this.closed.getAndSet(true)) {
                filterFactory.close(initResult);
            }
        }

        @Override
        public String toString() {
            return "Wrapper[" +
                    "filterFactory=" + filterFactory + ", " +
                    "filterDefinition=" + filterDefinition + ']';
        }

    }

    private final Map<String, Wrapper> initialized;

    public FilterChainFactory(PluginFactoryRegistry pfr, @Nullable List<NamedFilterDefinition> filterDefinitions) {
        @SuppressWarnings({ "unchecked", "rawtypes" })
        Class<FilterFactory<? super Object, ? super Object>> type = (Class) FilterFactory.class;
        PluginFactory<FilterFactory<? super Object, ? super Object>> pluginFactory = pfr.pluginFactory(type);
        if (filterDefinitions == null || filterDefinitions.isEmpty()) {
            this.initialized = Map.of();
        }
        else {
            FilterFactoryContext context = new FilterFactoryContext() {

                @Override
                public FilterDispatchExecutor filterDispatchExecutor() {
                    throw new IllegalStateException("no Filter Dispatch executor available at filter factory initialization time");
                }

                @Override
                public <P> P pluginInstance(Class<P> pluginClass, String instanceName) {
                    return pfr.pluginFactory(pluginClass).pluginInstance(instanceName);
                }
            };
            this.initialized = new LinkedHashMap<>(filterDefinitions.size());
            try {
                for (var fd : filterDefinitions) {
                    FilterFactory<? super Object, ? super Object> filterFactory = pluginFactory.pluginInstance(fd.type());
                    Class<?> configType = pluginFactory.configType(fd.type());
                    if (fd.config() == null || configType.isInstance(fd.config())) {
                        Wrapper uninitializedFilterFactory = new Wrapper(context, fd, filterFactory);
                        this.initialized.put(fd.name(), uninitializedFilterFactory);
                    }
                    else {
                        throw new PluginConfigurationException("Filter " + fd.name() + " accepts config of type " +
                                configType.getName() + " but provided with config of type " + fd.config().getClass().getName() + "]");
                    }
                }
            }
            catch (Exception e) {
                // close already initialized factories
                close();
                throw e;
            }
        }
    }

    @Override
    public void close() {
        RuntimeException firstThrown = null;
        // Close in reverse order of initialization
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

    /**
     * Creates and returns a new chain of filter instances.
     *
     * @return the new chain.
     */
    public List<FilterAndInvoker> createFilters(FilterFactoryContext context,
                                                @Nullable List<NamedFilterDefinition> filterChain) {
        if (filterChain == null) {
            return List.of();
        }
        return filterChain
                .stream()
                .flatMap(filterDefinition -> FilterAndInvoker.build(
                        new FilterOptions(filterDefinition.name(), TargetMessageClass.ALL),
                        initialized.get(filterDefinition.name()).create(context))
                        .stream())
                .toList();
    }
}
