/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.cluster;

import java.util.Set;

/**
 * Immutable partition assignment for a topic namespace. Implementations must be thread-safe
 * for concurrent reads from event loop threads.
 */
public interface PartitionAssignment {

    /** Total number of partitions in this assignment. */
    int partitionCount();

    /**
     * Returns the metadata for the given partition.
     *
     * @param partitionIndex partition index, must be in [0, partitionCount())
     * @return the partition info
     * @throws IndexOutOfBoundsException if partitionIndex is out of range
     */
    PartitionInfo partitionInfo(int partitionIndex);

    /**
     * Returns the set of partition indices for which the given node is the leader.
     *
     * @param nodeId the node identifier
     * @return set of partition indices led by the node, empty if none
     */
    Set<Integer> partitionsLedBy(int nodeId);
}
