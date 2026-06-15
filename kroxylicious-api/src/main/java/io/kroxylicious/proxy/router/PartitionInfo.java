/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.router;

import java.util.List;

/**
 * Partition topology: leader, replicas, and in-sync replicas.
 * All node references are virtual (already translated by the runtime).
 *
 * @param leader the partition leader
 * @param replicas all replicas of this partition
 * @param isr the in-sync replicas
 */
public record PartitionInfo(VirtualNode leader, List<VirtualNode> replicas, List<VirtualNode> isr) {
    public PartitionInfo {
        replicas = List.copyOf(replicas);
        isr = List.copyOf(isr);
    }
}
