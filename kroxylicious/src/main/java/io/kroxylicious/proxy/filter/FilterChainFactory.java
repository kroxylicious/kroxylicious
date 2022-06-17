/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.filter;

/**
 * Abstracts the creation of a chain of filter instances, hiding the configuration
 * required for instantiation at the point at which instances are created.
 * New instances are created during initialization of a downstream channel.
 */
public interface FilterChainFactory {

    /**
     * Create a new chain of filter instances
     * @return the new chain.
     */
    KrpcFilter[] createFilters();
}
