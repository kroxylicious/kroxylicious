/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.bootstrap;

import java.util.List;
import java.util.Set;
import java.util.StringJoiner;
import java.util.stream.Collectors;

import io.kroxylicious.proxy.config.Configuration;
import io.kroxylicious.proxy.config.FilterDefinition;
import io.kroxylicious.proxy.filter.FilterAndInvoker;
import io.kroxylicious.proxy.internal.filter.FilterContributorManager;

/**
 * Abstracts the creation of a chain of filter instances, hiding the configuration
 * required for instantiation at the point at which instances are created.
 * New instances are created during initialization of a downstream channel.
 */
public class FilterChainFactory {

    private final Configuration config;
    private final FilterContributorManager filterContributorManager;

    public FilterChainFactory(Configuration config, FilterContributorManager filterContributorManager) {
        this.config = config;
        this.filterContributorManager = filterContributorManager;
    }

    /**
     * Create a new chain of filter instances
     *
     * @return the new chain.
     */
    public List<FilterAndInvoker> createFilters() {

        List<FilterDefinition> filters = config.filters();
        if (filters == null || filters.isEmpty()) {
            return List.of();
        }
        final Set<String> filtersWithoutRequiredConfiguration = filters.stream().filter(f -> f.config() == null && filterContributorManager.requiresConfig(f.type())).map(FilterDefinition::type).collect(Collectors.toSet());
        if (!filtersWithoutRequiredConfiguration.isEmpty()) {
            StringJoiner joiner = new StringJoiner(", ", "[", "]");
            for (String s : filtersWithoutRequiredConfiguration) {
                joiner.add(s);
            }
            throw new IllegalStateException("Missing required config for " + joiner.toString());
        }
        return filters
                .stream()
                .map(f -> filterContributorManager.getFilter(f.type(), f.config()))
                .flatMap(filter -> FilterAndInvoker.build(filter).stream())
                .toList();
    }
}
