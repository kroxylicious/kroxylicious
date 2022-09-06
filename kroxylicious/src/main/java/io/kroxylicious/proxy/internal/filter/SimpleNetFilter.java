/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.filter;

import io.kroxylicious.proxy.bootstrap.FilterChainFactory;
import io.kroxylicious.proxy.filter.NetFilter;

/**
 * Implementation of {@link NetFilter} that is able to connect to a
 * single cluster, using a single, constant {@link FilterChainFactory}.
 */
public class SimpleNetFilter implements NetFilter {

    private final String remoteHost;
    private final int remotePort;
    private final FilterChainFactory filterChainFactory;

    public SimpleNetFilter(String remoteHost, int remotePort, FilterChainFactory filterChainFactory) {
        this.remoteHost = remoteHost;
        this.remotePort = remotePort;
        this.filterChainFactory = filterChainFactory;
    }

    @Override
    public void upstreamBroker(NetFilterContext context) {
        context.connect(remoteHost, remotePort, filterChainFactory.createFilters());
    }
}
