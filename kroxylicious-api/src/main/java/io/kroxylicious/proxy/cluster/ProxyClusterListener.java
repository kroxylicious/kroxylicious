/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.cluster;

import java.util.OptionalInt;
import java.util.Set;

/** Callback for proxy cluster state changes. */
public interface ProxyClusterListener {

    /**
     * Invoked when the set of cluster members changes.
     *
     * @param members the new membership set
     */
    default void onMembershipChanged(Set<ProxyClusterMember> members) {
    }

    /**
     * Invoked when the leader for a virtual cluster changes.
     *
     * @param virtualClusterName the virtual cluster whose leader changed
     * @param newLeaderId the new leader's node ID, or empty if no leader
     */
    default void onVirtualClusterLeaderChanged(
                                               String virtualClusterName,
                                               OptionalInt newLeaderId) {
    }

    /**
     * Invoked when the partition assignment for a virtual cluster namespace changes.
     *
     * @param virtualClusterName the virtual cluster
     * @param namespace the topic namespace (e.g. {@code __consumer_offsets})
     * @param assignment the new partition assignment
     */
    default void onPartitionAssignmentChanged(
                                              String virtualClusterName,
                                              String namespace,
                                              PartitionAssignment assignment) {
    }
}
