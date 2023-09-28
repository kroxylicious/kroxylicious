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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.proxy.config.FilterDefinition;
import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.filter.FilterCreationContext;
import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.filter.InvalidFilterConfigurationException;

public class FilterFactoryManager {
    private static final Logger logger = LoggerFactory.getLogger(FilterDefinition.class);

    public static final FilterFactoryManager INSTANCE = new FilterFactoryManager();
    private final Map<String, FilterFactory> filterFactories;

    private FilterFactoryManager() {
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

    public boolean validateConfig(String typeName, Object config) {
        try {
            FilterFactory factory = filterFactories.get(typeName);
            factory.validateConfiguration(config);
            return true;
        }
        catch (InvalidFilterConfigurationException e) {
            logger.warn("Filter with type: {}, failed to validate configuration", typeName, e);
            return false;
        }
    }

    private FilterFactory getFactory(String typeName) {
        FilterFactory factory = filterFactories.get(typeName);
        if (factory == null) {
            throw new IllegalArgumentException("no FilterFactory registered for typeName: " + typeName);
        }
        return factory;
    }

}
