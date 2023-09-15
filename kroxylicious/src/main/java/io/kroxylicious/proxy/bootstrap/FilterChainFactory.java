/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.bootstrap;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import io.kroxylicious.proxy.config.Configuration;
import io.kroxylicious.proxy.config.FilterDefinition;
import io.kroxylicious.proxy.filter.FilterAndInvoker;
import io.kroxylicious.proxy.filter.FilterContributor;
import io.kroxylicious.proxy.internal.filter.NettyFilterContext;
import io.kroxylicious.proxy.service.ContributionManager;

/**
 * Abstracts the creation of a chain of filter instances, hiding the configuration
 * required for instantiation at the point at which instances are created.
 * New instances are created during initialization of a downstream channel.
 */
public class FilterChainFactory {

    private final Configuration config;

    public FilterChainFactory(Configuration config) {
        this.config = Objects.requireNonNull(config);
    }

    public static boolean validateFilterConfiguration(List<FilterDefinition> filters) {
        if (Objects.isNull(filters) || filters.isEmpty()) {
            return true;
        }
        final Set<String> filtersWithoutRequiredConfiguration = filters.stream()
                .filter(f -> f.config() == null)
                .map(FilterDefinition::type)
                .filter(type -> ContributionManager.INSTANCE.getDefinition(FilterContributor.class, type).configurationRequired())
                .collect(Collectors.toSet());
        if (!filtersWithoutRequiredConfiguration.isEmpty()) {
            throw new IllegalStateException(filtersWithoutRequiredConfiguration.stream()
                    .collect(Collectors.joining(", ", "Missing required config for [", "]")));
        }
        return true;
    }

    /**
     * Create a new chain of filter instances
     *
     * @return the new chain.
     */
    public List<FilterAndInvoker> createFilters(NettyFilterContext context) {
        List<FilterDefinition> filters = config.filters();
        if (filters == null || filters.isEmpty()) {
            return List.of();
        }
        validateFilterConfiguration(filters);
        return filters
                .stream()
                .map(f -> ContributionManager.INSTANCE.getInstance(FilterContributor.class, f.type(), context.wrap(f.config())))
                .flatMap(filter -> FilterAndInvoker.build(filter).stream())
                .toList();
    }
}