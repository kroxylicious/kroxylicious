/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.cluster;

import java.util.List;
import java.util.OptionalInt;

import static java.util.Objects.requireNonNull;

/**
 * Per-partition metadata within a {@link PartitionAssignment}.
 * Models the leader, replicas, in-sync replicas, and leader epoch for a single partition.
 */
public record PartitionInfo(
                            int partitionIndex,
                            int leaderEpoch,
                            OptionalInt leader,
                            List<Integer> replicas,
                            List<Integer> isr) {

    public PartitionInfo {
        if (partitionIndex < 0) {
            throw new IllegalArgumentException("partitionIndex must be non-negative, got " + partitionIndex);
        }
        if (leaderEpoch < 0) {
            throw new IllegalArgumentException("leaderEpoch must be non-negative, got " + leaderEpoch);
        }
        requireNonNull(leader, "leader");
        requireNonNull(replicas, "replicas");
        requireNonNull(isr, "isr");
        replicas = List.copyOf(replicas);
        isr = List.copyOf(isr);
    }
}
