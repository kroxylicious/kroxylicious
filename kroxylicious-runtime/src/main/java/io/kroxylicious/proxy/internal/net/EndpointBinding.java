/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.net;

import io.kroxylicious.proxy.service.HostPort;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * An endpoint binding.
 */
public interface EndpointBinding {
    /**
     * The endpoint listener.
     *
     * @return virtual cluster.
     */
    EndpointGateway endpointGateway();

    /**
     * The upstream target of this binding.
     *
     * @return upstream target.
     */
    HostPort upstreamTarget();

    /**
     * Returns {@code true} if this binding is a temporary placeholder whose upstream target
     * must only be used for initial topology discovery, not for serving real client traffic.
     *
     * <p>A {@code true} value indicates that the proxy has bound a port for a broker node ID
     * but does not yet know that broker's real upstream address. The upstream target points at
     * the cluster's bootstrap server as a stand-in. Once the real topology is learned (via a
     * Metadata response), the registry replaces this binding with a {@link BrokerEndpointBinding}
     * that points at the actual broker address.
     *
     * <p><strong>Note for callers:</strong> this flag can be {@code true} for virtual clusters
     * using either direct or dynamic routing, because {@link MetadataDiscoveryBrokerEndpointBinding}
     * is created for any virtual cluster whose gateway declares per-node ports. Callers that
     * install {@link io.kroxylicious.proxy.internal.filter.impl.EagerMetadataLearner} in
     * response to this flag <em>must</em> additionally verify that the virtual cluster uses
     * {@link io.kroxylicious.proxy.internal.routing.DirectRouting}, because
     * {@code EagerMetadataLearner} relies on {@link #upstreamTarget()} which throws for
     * dynamic routing.
     *
     * @return {@code true} if the upstream target is restricted to metadata discovery.
     * @see MetadataDiscoveryBrokerEndpointBinding
     */
    default boolean restrictUpstreamToMetadataDiscovery() {
        return false;
    }

    /**
     * Returns the broker node id associated with this endpoint.  If the endpoint
     * is being used for bootstrapping, null will be returned instead.
     * @return node id or null.
     */
    @Nullable
    Integer nodeId();
}
