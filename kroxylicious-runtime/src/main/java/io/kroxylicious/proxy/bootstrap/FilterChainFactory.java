/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.bootstrap;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

import io.kroxylicious.proxy.config.FilterDefinition;
import io.kroxylicious.proxy.config.PluginFactory;
import io.kroxylicious.proxy.config.PluginFactoryRegistry;
import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.filter.FilterAndInvoker;
import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Abstracts the creation of a chain of filter instances, hiding the configuration
 * required for instantiation at the point at which instances are created.
 * New instances are created during initialization of a downstream channel.
 */
public class FilterChainFactory {

    record UninitializedFilterFactory(String instanceName, FilterFactory<? super Object, ? super Object> filterFactory, Object config) {

        private InitializedFilterFactory initialize(FilterFactoryContext context) {
            Object result;
            try {
                result = filterFactory.initialize(context, config);
            }
            catch (Exception e) {
                throw new PluginConfigurationException("Exception initializing filter factory " + instanceName + " with config " + config + ": " + e.getMessage(), e);
            }
            return new InitializedFilterFactory(filterFactory, result);
        }
    }

    record InitializedFilterFactory(FilterFactory<? super Object, ? super Object> filterFactory, Object initResult) {
        public Filter create(FilterFactoryContext context) {
            try {
                return filterFactory().createFilter(context, initResult);
            }
            catch (Exception e) {
                throw new PluginConfigurationException("Exception instantiating filter using factory " + filterFactory, e);
            }
        }
    }

    /**
     * A constant Class type with non-raw {@code FilterFactory} type parameter,
     * for the avoidance of raw type warnings.
     */

    private final List<InitializedFilterFactory> initialized;

    public FilterChainFactory(PluginFactoryRegistry pfr, List<FilterDefinition> filterDefinitions) {
        @SuppressWarnings({ "unchecked", "rawtypes" })
        Class<FilterFactory<? super Object, ? super Object>> type = (Class) FilterFactory.class;
        PluginFactory<FilterFactory<? super Object, ? super Object>> pluginFactory = pfr.pluginFactory(type);
        if (filterDefinitions == null || ((Collection<FilterDefinition>) filterDefinitions).isEmpty()) {
            this.initialized = List.of();
        }
        else {
            FilterFactoryContext context = new FilterFactoryContext() {
                @Override
                public ScheduledExecutorService eventLoop() {
                    return null;
                }

                @Override
                public <P> @NonNull P pluginInstance(@NonNull Class<P> pluginClass, @NonNull String instanceName) {
                    return pfr.pluginFactory(pluginClass).pluginInstance(instanceName);
                }
            };
            this.initialized = filterDefinitions.stream()
                    .map(fd -> {
                        FilterFactory<? super Object, ? super Object> filterFactory = pluginFactory.pluginInstance(fd.type());
                        Class<?> configType = pluginFactory.configType(fd.type());
                        if (fd.config() == null || configType.isInstance(fd.config())) {
                            return new UninitializedFilterFactory(fd.type(), filterFactory, fd.config());
                        }
                        else {
                            throw new PluginConfigurationException("accepts config of type " +
                                    configType.getName() + " but provided with config of type " + fd.config().getClass().getName() + "]");
                        }
                    })
                    .map(uff -> uff.initialize(context))
                    .toList();
        }
    }

    /**
     * Creates and returns a new chain of filter instances.
     *
     * @return the new chain.
     */
    public List<FilterAndInvoker> createFilters(FilterFactoryContext context) {
        return initialized
                .stream()
                .map(pair -> pair.create(context))
                .flatMap(filter -> FilterAndInvoker.build(filter).stream())
                .toList();
    }
}
