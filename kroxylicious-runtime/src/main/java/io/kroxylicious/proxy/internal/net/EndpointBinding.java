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
     * If set true, the upstream target must only be used for metadata discovery.
     *
     * @return true if the target must be restricted to metadata discovery.
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
