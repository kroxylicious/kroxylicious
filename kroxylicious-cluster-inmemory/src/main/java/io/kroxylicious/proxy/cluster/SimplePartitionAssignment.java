/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.cluster;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.OptionalInt;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/** Immutable {@link PartitionAssignment} backed by an array of {@link PartitionInfo}. */
public final class SimplePartitionAssignment implements PartitionAssignment {

    private final PartitionInfo[] partitions;

    /**
     * Creates an assignment from an explicit array of partition info.
     *
     * @param partitions the per-partition metadata, indexed by partition index
     */
    public SimplePartitionAssignment(PartitionInfo... partitions) {
        requireNonNull(partitions, "partitions");
        this.partitions = Arrays.copyOf(partitions, partitions.length);
    }

    /**
     * Creates a range-based assignment, distributing partitions across nodes using the same
     * algorithm as Kafka's {@code RangeAssignor}: contiguous ranges of partitions assigned
     * to nodes in sorted order. Each partition has a single replica which is also leader
     * and sole ISR member. Leader epoch is set to 1.
     *
     * @param partitionCount total number of partitions
     * @param sortedNodeIds node IDs in ascending order
     * @return the partition assignment
     */
    public static SimplePartitionAssignment rangeAssignment(
                                                            int partitionCount,
                                                            List<Integer> sortedNodeIds) {
        if (partitionCount < 0) {
            throw new IllegalArgumentException("partitionCount must be non-negative, got " + partitionCount);
        }
        requireNonNull(sortedNodeIds, "sortedNodeIds");
        if (sortedNodeIds.isEmpty()) {
            throw new IllegalArgumentException("sortedNodeIds must not be empty");
        }
        var infos = new PartitionInfo[partitionCount];
        int nodeCount = sortedNodeIds.size();
        for (int i = 0; i < partitionCount; i++) {
            int ownerIndex = i * nodeCount / partitionCount;
            int ownerId = sortedNodeIds.get(ownerIndex);
            infos[i] = new PartitionInfo(
                    i,
                    1,
                    OptionalInt.of(ownerId),
                    List.of(ownerId),
                    List.of(ownerId));
        }
        return new SimplePartitionAssignment(infos);
    }

    @Override
    public int partitionCount() {
        return partitions.length;
    }

    @Override
    public PartitionInfo partitionInfo(int partitionIndex) {
        if (partitionIndex < 0 || partitionIndex >= partitions.length) {
            throw new IndexOutOfBoundsException(
                    "partitionIndex " + partitionIndex + " out of range [0, " + partitions.length + ")");
        }
        return partitions[partitionIndex];
    }

    @Override
    public Set<Integer> partitionsLedBy(int nodeId) {
        var result = new LinkedHashSet<Integer>();
        for (var info : partitions) {
            if (info.leader().isPresent() && info.leader().getAsInt() == nodeId) {
                result.add(info.partitionIndex());
            }
        }
        return Collections.unmodifiableSet(result);
    }
}
