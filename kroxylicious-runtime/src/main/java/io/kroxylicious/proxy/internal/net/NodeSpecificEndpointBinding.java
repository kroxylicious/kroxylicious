/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.net;

import java.util.Objects;

public interface NodeSpecificEndpointBinding extends EndpointBinding {
    @Override
    @SuppressWarnings("java:S6207")
    // method's return annotation differs from that of the interface
    Integer nodeId();

    default boolean refersToSameVirtualClusterAndNode(NodeSpecificEndpointBinding other) {
        return Objects.equals(other.nodeId(), this.nodeId()) && Objects.equals(other.endpointGateway(), this.endpointGateway());
    }
}
