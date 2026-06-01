/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.cluster;

import java.util.OptionalInt;
import java.util.Set;

/**
 * Proxy cluster membership, per-virtual-cluster leadership, and partition assignments.
 *
 * <h2>Thread safety</h2>
 * <p>Implementations must be safe for concurrent reads from Netty event loop threads.
 * State is written by the cluster leader (or test code) and read by event loop threads
 * servicing client requests.</p>
 *
 * <h2>Consistency requirements</h2>
 * <ul>
 *   <li><b>Membership</b> — all members must converge on the same view before acting on it.
 *       A functioning member must not be incorrectly removed.</li>
 *   <li><b>Leader election</b> — at most one leader per virtual cluster at any time (no split-brain).
 *       If the leader fails, a new leader is eventually elected.</li>
 *   <li><b>Partition assignment</b> — linearisable or epoch-fenced. Every partition assigned to
 *       exactly one live member. All members must agree on the assignment before router.</li>
 * </ul>
 *
 * <p>Eventual-consistency systems (e.g. Kubernetes leases) may not satisfy these requirements
 * without epoch-based fencing and stale-read rejection.</p>
 */
public interface ProxyCluster {

    /** The node ID of this proxy instance. */
    int localNodeId();

    /** The cluster member record for this proxy instance. */
    ProxyClusterMember localMember();

    /** All current members of the cluster. */
    Set<ProxyClusterMember> members();

    /**
     * Returns the leader node ID for the given virtual cluster.
     *
     * @param virtualClusterName the virtual cluster name
     * @return the leader's node ID, or empty if no leader is currently elected
     */
    OptionalInt leaderForVirtualCluster(String virtualClusterName);

    /**
     * Returns whether this proxy instance is the leader for the given virtual cluster.
     *
     * @param virtualClusterName the virtual cluster name
     * @return true if this instance is the leader
     */
    default boolean isLeaderForVirtualCluster(String virtualClusterName) {
        return leaderForVirtualCluster(virtualClusterName)
                .orElse(-1) == localNodeId();
    }

    /**
     * Returns the partition assignment for a namespace within a virtual cluster.
     *
     * @param virtualClusterName the virtual cluster name
     * @param namespace the topic namespace (e.g. {@code __consumer_offsets}, {@code __transaction_state})
     * @return the current partition assignment
     */
    PartitionAssignment partitionAssignment(
                                            String virtualClusterName,
                                            String namespace);

    /**
     * Registers a listener for cluster state changes.
     *
     * @param listener the listener to add
     */
    void addListener(ProxyClusterListener listener);

    /**
     * Removes a previously registered listener.
     *
     * @param listener the listener to remove
     */
    void removeListener(ProxyClusterListener listener);
}
