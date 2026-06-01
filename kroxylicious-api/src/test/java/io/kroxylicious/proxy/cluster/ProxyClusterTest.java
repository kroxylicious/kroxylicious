/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.cluster;

import java.util.OptionalInt;
import java.util.Set;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ProxyClusterTest {

    @Test
    void isLeaderReturnsTrueWhenLocalNodeIsLeader() {
        ProxyCluster cluster = stubCluster(5, OptionalInt.of(5));

        assertThat(cluster.isLeaderForVirtualCluster("vc1")).isTrue();
    }

    @Test
    void isLeaderReturnsFalseWhenDifferentNodeIsLeader() {
        ProxyCluster cluster = stubCluster(5, OptionalInt.of(99));

        assertThat(cluster.isLeaderForVirtualCluster("vc1")).isFalse();
    }

    @Test
    void isLeaderReturnsFalseWhenNoLeaderElected() {
        ProxyCluster cluster = stubCluster(5, OptionalInt.empty());

        assertThat(cluster.isLeaderForVirtualCluster("vc1")).isFalse();
    }

    private static ProxyCluster stubCluster(int localNodeId, OptionalInt leaderNodeId) {
        return new ProxyCluster() {
            @Override
            public int localNodeId() {
                return localNodeId;
            }

            @Override
            public ProxyClusterMember localMember() {
                return null;
            }

            @Override
            public Set<ProxyClusterMember> members() {
                return Set.of();
            }

            @Override
            public OptionalInt leaderForVirtualCluster(String virtualClusterName) {
                return leaderNodeId;
            }

            @Override
            public PartitionAssignment partitionAssignment(String virtualClusterName, String namespace) {
                return null;
            }

            @Override
            public void addListener(ProxyClusterListener listener) {
            }

            @Override
            public void removeListener(ProxyClusterListener listener) {
            }
        };
    }
}
