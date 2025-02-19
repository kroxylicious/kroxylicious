/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.net;

import io.kroxylicious.proxy.service.HostPort;

/**
 * A binding to a virtual cluster.
 */
public interface VirtualClusterBinding {
    /**
     * The virtual cluster.
     *
     * @return virtual cluster.
     */
    EndpointListener endpointListener();

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
}
