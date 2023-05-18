/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.bootstrap;

import java.util.List;

import io.kroxylicious.proxy.config.Configuration;
import io.kroxylicious.proxy.config.VirtualCluster;
import io.kroxylicious.proxy.filter.KrpcFilter;
import io.kroxylicious.proxy.internal.filter.FilterContributorManager;

/**
 * Abstracts the creation of a chain of filter instances, hiding the configuration
 * required for instantiation at the point at which instances are created.
 * New instances are created during initialization of a downstream channel.
 */
public class FilterChainFactory {

    private final Configuration config;
    private final VirtualCluster virtualCluster;

    public FilterChainFactory(Configuration config, VirtualCluster virtualCluster) {
        this.config = config;
        this.virtualCluster = virtualCluster;
    }

    /**
     * Create a new chain of filter instances
     *
     * @return the new chain.
     */
    public List<KrpcFilter> createFilters() {
        FilterContributorManager filterContributorManager = FilterContributorManager.getInstance();

        return config.filters()
                .stream()
                .map(f -> filterContributorManager.getFilter(f.type(), virtualCluster.getClusterEndpointProvider(), f.config()))
                .toList();
    }
}
