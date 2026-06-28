/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.util;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Represents a node in a virtual cluster, identified by its cluster name and an optional node ID.
 * This is used to track nodes within a virtual cluster context.
 */
public record VirtualClusterNode(String clusterName, @Nullable Integer nodeId) {

    @Override
    public String toString() {
        return "VirtualClusterNode{" +
                "clusterName='" + clusterName + '\'' +
                ", nodeId=" + nodeId +
                '}';
    }
}
