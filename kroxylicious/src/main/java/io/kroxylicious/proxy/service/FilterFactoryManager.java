/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.service;

import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;

import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.filter.FilterCreationContext;
import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.filter.InvalidFilterConfigurationException;

public class FilterFactoryManager {

    @Deprecated
    public static final FilterFactoryManager INSTANCE = new FilterFactoryManager();
    private final Map<String, FilterFactory> filterFactories;

    public FilterFactoryManager() {
        ServiceLoader<FilterFactory> factories = ServiceLoader.load(FilterFactory.class);
        HashMap<String, FilterFactory> nameToFactory = new HashMap<>();
        for (FilterFactory factory : factories) {
            Class<?> serviceType = factory.filterType();
            Set<String> names = Set.of(serviceType.getName(), serviceType.getSimpleName());
            names.forEach(name -> {
                FilterFactory<?, ?> previous = nameToFactory.put(name, factory);
                if (previous != null) {
                    throw new IllegalStateException("more than one FilterFactory offers Filter named: " + name);
                }
            });
        }
        filterFactories = nameToFactory;
    }

    public Filter createInstance(String typeName, FilterCreationContext constructionContext, Object config) {
        return getFactory(typeName).createFilter(constructionContext, config);
    }

    public Class<?> getConfigType(String typeName) {
        return getFactory(typeName).configType();
    }

    /**
     * Validates the config for a filter factory with the given name
     * @param typeName The name
     * @param config The config
     * @throws InvalidFilterConfigurationException If the factory rejects the given config
     */
    public void validateConfig(String typeName, Object config) {
        FilterFactory factory = filterFactories.get(typeName);
        factory.validateConfiguration(config);
    }

    private FilterFactory getFactory(String typeName) {
        FilterFactory factory = filterFactories.get(typeName);
        if (factory == null) {
            throw new IllegalArgumentException("no FilterFactory registered for typeName: " + typeName);
        }
        return factory;
    }

}
