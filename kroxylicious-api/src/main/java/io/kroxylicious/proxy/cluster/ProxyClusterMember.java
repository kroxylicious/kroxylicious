/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.cluster;

import java.util.List;

import edu.umd.cs.findbugs.annotations.Nullable;

import static java.util.Objects.requireNonNull;

/** A member of the proxy cluster. */
public record ProxyClusterMember(
                                 int nodeId,
                                 @Nullable String rackId,
                                 List<ProxyClusterEndpoint> endpoints) {

    public ProxyClusterMember {
        if (nodeId < 1) {
            throw new IllegalArgumentException("nodeId must be positive, got " + nodeId);
        }
        requireNonNull(endpoints, "endpoints");
        if (endpoints.isEmpty()) {
            throw new IllegalArgumentException("endpoints must not be empty");
        }
        endpoints = List.copyOf(endpoints);
    }
}
