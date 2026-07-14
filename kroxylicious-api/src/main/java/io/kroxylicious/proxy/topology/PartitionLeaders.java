/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.topology;

import java.util.Optional;

/**
 * A snapshot of partition leader assignments, returned by
 * {@link TopologyService#leaders}.
 *
 * <p>This is a self-contained result — callers should use it
 * directly rather than querying the topology cache separately.
 * The snapshot is immutable and safe to use across async
 * callback boundaries.</p>
 */
public interface PartitionLeaders {

    /**
     * Returns the leader for the given topic-partition, or empty
     * if the leader was not discovered (e.g. the topic does not
     * exist, or the partition has no leader).
     *
     * @param topicName the topic name
     * @param partitionIndex the partition index
     * @return the leader's virtual node, or empty
     */
    Optional<EndpointType.VirtualNode> leaderOf(String topicName, int partitionIndex);
}
