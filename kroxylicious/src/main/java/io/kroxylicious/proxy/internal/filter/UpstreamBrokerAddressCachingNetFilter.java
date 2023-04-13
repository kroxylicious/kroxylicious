/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.filter;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import io.kroxylicious.proxy.bootstrap.FilterChainFactory;
import io.kroxylicious.proxy.filter.KrpcFilter;
import io.kroxylicious.proxy.filter.MetadataResponseFilter;
import io.kroxylicious.proxy.filter.NetFilter;
import io.kroxylicious.proxy.service.ClusterEndpointProvider;
import io.kroxylicious.proxy.service.HostPort;

/**
 * Temporary implementation of NetFilter that uses the port number from the local end of the
 * downstream connection to identify the virtual clusters broker.  It then uses this to identify
 * the upstream broker address from a cache built from a previous connection's metadata response.
 */
public class UpstreamBrokerAddressCachingNetFilter implements NetFilter {

    private final HostPort targetClusterBootstrap;
    private final FilterChainFactory filterChainFactory;
    private final ClusterEndpointProvider endpointProvider;

    private final Map<Integer, HostPort> upstreamBrokers = new ConcurrentHashMap<>();

    public UpstreamBrokerAddressCachingNetFilter(HostPort targetClusterBootstrap, FilterChainFactory filterChainFactory,
                                                 ClusterEndpointProvider endpointProvider) {
        this.targetClusterBootstrap = targetClusterBootstrap;
        this.filterChainFactory = filterChainFactory;
        this.endpointProvider = endpointProvider;
    }

    @Override
    public void selectServer(NetFilterContext context) {
        var filters = new ArrayList<>(Arrays.stream(filterChainFactory.createFilters()).toList());

        // Add a filter to the *end of the chain* that gathers the true nodeId/upstream broker mapping.
        filters.add((MetadataResponseFilter) (header, response, filterContext) -> {
            response.brokers().forEach(b -> upstreamBrokers.put(b.nodeId(), new HostPort(b.host(), b.port())));
            filterContext.forwardResponse(response);
        });

        var endpointMatchResult = endpointProvider.hasMatchingEndpoint(context.sniHostname(),
                ((InetSocketAddress) context.localAddress()).getPort());

        HostPort target;
        if (endpointMatchResult.matched() && endpointMatchResult.nodeId() != null) {
            target = upstreamBrokers.getOrDefault(endpointMatchResult.nodeId(), targetClusterBootstrap);
        }
        else {
            target = targetClusterBootstrap;
        }

        context.initiateConnect(target.host(), target.port(), filters.toArray(new KrpcFilter[0]));
    }

}
