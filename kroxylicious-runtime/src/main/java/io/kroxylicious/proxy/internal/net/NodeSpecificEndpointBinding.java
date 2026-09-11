/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.net;

import java.util.Objects;

/**
 * An endpoint binding that targets a specific Kafka node, and so always has a non-null nodeId.
 */
public interface NodeSpecificEndpointBinding extends EndpointBinding {
    @Override
    @SuppressWarnings("java:S6207")
    // method's return annotation differs from that of the interface
    Integer nodeId();

    /**
     * Determines whether another binding refers to the same virtual cluster gateway and node as this one.
     *
     * @param other binding to compare against
     * @return true if {@code other} has the same nodeId and endpoint gateway as this binding
     */
    default boolean refersToSameVirtualClusterAndNode(NodeSpecificEndpointBinding other) {
        return Objects.equals(other.nodeId(), this.nodeId()) && Objects.equals(other.endpointGateway(), this.endpointGateway());
    }
}
