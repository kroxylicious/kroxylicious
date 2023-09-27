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
import io.kroxylicious.proxy.filter.FilterConstructContext;
import io.kroxylicious.proxy.filter.FilterFactory;

public class FilterContributionManager {

    public static final FilterContributionManager INSTANCE = new FilterContributionManager();
    private final Map<String, FilterFactory<?>> filterContributors;

    private FilterContributionManager() {
        ServiceLoader<FilterFactory> contributors = ServiceLoader.load(FilterFactory.class);
        HashMap<String, FilterFactory<?>> nameToContributor = new HashMap<>();
        for (FilterFactory<?> contributor : contributors) {
            Class<?> serviceType = contributor.getServiceType();
            Set<String> names = Set.of(serviceType.getName(), serviceType.getSimpleName());
            names.forEach(name -> {
                FilterFactory<?> previous = nameToContributor.put(name, contributor);
                if (previous != null) {
                    throw new IllegalStateException("more than one FilterContributor offers Filter named: " + name);
                }
            });
        }
        filterContributors = nameToContributor;
    }

    public Filter createInstance(String typeName, FilterConstructContext<?> constructionContext) {
        return getContributor(typeName).createInstance((FilterConstructContext) constructionContext);
    }

    public Class<?> getConfigType(String typeName) {
        return getContributor(typeName).getConfigType();
    }

    public boolean validateConfig(String typeName, Object config) {
        FilterFactory<?> contributor = getContributor(typeName);
        return !contributor.requiresConfiguration() || config != null;
    }

    private FilterFactory<?> getContributor(String typeName) {
        FilterFactory<?> contributor = filterContributors.get(typeName);
        if (contributor == null) {
            throw new IllegalArgumentException("no FilterContributor registered for typeName: " + typeName);
        }
        return contributor;
    }

}
