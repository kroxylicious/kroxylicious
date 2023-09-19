/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.bootstrap;

import java.util.List;
import java.util.Objects;

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
        Objects.requireNonNull(config);
        this.config = config;
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
        return filters
                .stream()
                .map(f -> ContributionManager.INSTANCE.getInstance(FilterContributor.class, f.type(), context.wrap(f.config())))
                .flatMap(filter -> FilterAndInvoker.build(filter).stream())
                .toList();
    }
}