/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.bootstrap;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import io.kroxylicious.proxy.config.Configuration;
import io.kroxylicious.proxy.config.FilterDefinition;
import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.filter.FilterAndInvoker;
import io.kroxylicious.proxy.filter.FilterContributor;
import io.kroxylicious.proxy.internal.filter.NettyFilterContext;
import io.kroxylicious.proxy.service.Context;
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

    /**
     * Validate that a collection of FilterDefinitions contains all the required configuration to configure the filter.
     * @param filterDefinitions the candidates to validate.
     * @return the Set of filter names which fail validation.
     */
    public static Set<String> validateFilterConfiguration(Collection<FilterDefinition> filterDefinitions) {
        if (filterDefinitions == null || filterDefinitions.isEmpty()) {
            return Set.of();
        }
        return filterDefinitions.stream()
                .filter(Predicate.not(FilterDefinition::isDefinitionValid))
                .map(FilterDefinition::type)
                .collect(Collectors.toSet());
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
                .map(f -> {
                    Context<Object> wrap = context.wrap(f.config());
                    return (Filter) ContributionManager.INSTANCE.getInstance(FilterContributor.class, f.type(), wrap);
                })
                .flatMap(filter -> FilterAndInvoker.build(filter).stream())
                .toList();
    }
}
