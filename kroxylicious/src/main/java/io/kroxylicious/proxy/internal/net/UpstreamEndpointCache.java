/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.net;

import java.util.Map;

import io.kroxylicious.proxy.service.HostPort;

public interface UpstreamEndpointCache {
    HostPort getUpstreamClusterAddressForNode(int nodeId);

    HostPort updateUpstreamClusterAddressForNode(int nodeId, HostPort replacement);

    boolean updateUpstreamClusterAddresses(Map<Integer, HostPort> nodeMap);
}
