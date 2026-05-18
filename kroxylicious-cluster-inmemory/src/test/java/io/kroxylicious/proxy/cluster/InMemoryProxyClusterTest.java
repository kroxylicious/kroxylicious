/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.cluster;

import java.util.ArrayList;
import java.util.List;
import java.util.OptionalInt;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class InMemoryProxyClusterTest {

    private static final ProxyClusterEndpoint ENDPOINT_1 = new ProxyClusterEndpoint("host1", 9092, Transport.TCP);
    private static final ProxyClusterEndpoint ENDPOINT_2 = new ProxyClusterEndpoint("host2", 9092, Transport.TLS);
    private static final ProxyClusterMember MEMBER_1 = new ProxyClusterMember(1, null, List.of(ENDPOINT_1));
    private static final ProxyClusterMember MEMBER_2 = new ProxyClusterMember(2, "rack-b", List.of(ENDPOINT_2));

    private InMemoryClusterState state;
    private InMemoryProxyCluster cluster1;
    private InMemoryProxyCluster cluster2;

    @BeforeEach
    void setUp() {
        state = new InMemoryClusterState();
        state.setMembers(Set.of(MEMBER_1, MEMBER_2));
        cluster1 = new InMemoryProxyCluster(1, state);
        cluster2 = new InMemoryProxyCluster(2, state);
    }

    @Test
    void shouldReturnLocalNodeId() {
        assertThat(cluster1.localNodeId()).isEqualTo(1);
        assertThat(cluster2.localNodeId()).isEqualTo(2);
    }

    @Test
    void shouldReturnLocalMember() {
        assertThat(cluster1.localMember()).isEqualTo(MEMBER_1);
        assertThat(cluster2.localMember()).isEqualTo(MEMBER_2);
    }

    @Test
    void shouldThrowWhenLocalMemberNotInMembership() {
        var orphan = new InMemoryProxyCluster(99, state);

        assertThatThrownBy(orphan::localMember)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("99");
    }

    @Test
    void shouldShareMembershipAcrossInstances() {
        assertThat(cluster1.members()).isEqualTo(cluster2.members());
        assertThat(cluster1.members()).containsExactlyInAnyOrder(MEMBER_1, MEMBER_2);
    }

    @Test
    void shouldSeeMembershipChanges() {
        var member3 = new ProxyClusterMember(3, null,
                List.of(new ProxyClusterEndpoint("host3", 9092, Transport.TCP)));
        state.setMembers(Set.of(MEMBER_1, MEMBER_2, member3));

        assertThat(cluster1.members()).hasSize(3);
        assertThat(cluster2.members()).hasSize(3);
    }

    @Test
    void shouldReturnPerVcLeadership() {
        state.setLeaderForVirtualCluster("vc-a", 1);
        state.setLeaderForVirtualCluster("vc-b", 2);

        assertThat(cluster1.leaderForVirtualCluster("vc-a")).hasValue(1);
        assertThat(cluster1.leaderForVirtualCluster("vc-b")).hasValue(2);
        assertThat(cluster1.isLeaderForVirtualCluster("vc-a")).isTrue();
        assertThat(cluster1.isLeaderForVirtualCluster("vc-b")).isFalse();

        assertThat(cluster2.isLeaderForVirtualCluster("vc-a")).isFalse();
        assertThat(cluster2.isLeaderForVirtualCluster("vc-b")).isTrue();
    }

    @Test
    void shouldReturnEmptyForUnknownVcLeader() {
        assertThat(cluster1.leaderForVirtualCluster("unknown")).isEmpty();
        assertThat(cluster1.isLeaderForVirtualCluster("unknown")).isFalse();
    }

    @Test
    void shouldReturnPartitionAssignment() {
        var assignment = SimplePartitionAssignment.rangeAssignment(4, List.of(1, 2));
        state.setPartitionAssignment("vc-a", "__consumer_offsets", assignment);

        assertThat(cluster1.partitionAssignment("vc-a", "__consumer_offsets")).isSameAs(assignment);
        assertThat(cluster2.partitionAssignment("vc-a", "__consumer_offsets")).isSameAs(assignment);
    }

    @Test
    void shouldReturnNullForUnknownAssignment() {
        assertThat(cluster1.partitionAssignment("unknown", "__consumer_offsets")).isNull();
    }

    @Test
    void shouldSupportIndependentNamespaces() {
        var consumerOffsets = SimplePartitionAssignment.rangeAssignment(50, List.of(1, 2));
        var txnState = SimplePartitionAssignment.rangeAssignment(16, List.of(1, 2));
        state.setPartitionAssignment("vc-a", "__consumer_offsets", consumerOffsets);
        state.setPartitionAssignment("vc-a", "__transaction_state", txnState);

        assertThat(cluster1.partitionAssignment("vc-a", "__consumer_offsets").partitionCount()).isEqualTo(50);
        assertThat(cluster1.partitionAssignment("vc-a", "__transaction_state").partitionCount()).isEqualTo(16);
    }

    @Test
    void shouldFireMembershipListener() {
        var received = new ArrayList<Set<ProxyClusterMember>>();
        cluster1.addListener(new ProxyClusterListener() {
            @Override
            public void onMembershipChanged(Set<ProxyClusterMember> members) {
                received.add(members);
            }
        });

        state.setMembers(Set.of(MEMBER_1));

        assertThat(received).hasSize(1);
        assertThat(received.get(0)).containsExactly(MEMBER_1);
    }

    @Test
    void shouldFireVcLeaderListener() {
        var vcNames = new ArrayList<String>();
        var leaderIds = new ArrayList<OptionalInt>();
        cluster1.addListener(new ProxyClusterListener() {
            @Override
            public void onVirtualClusterLeaderChanged(
                                                      String virtualClusterName,
                                                      OptionalInt newLeaderId) {
                vcNames.add(virtualClusterName);
                leaderIds.add(newLeaderId);
            }
        });

        state.setLeaderForVirtualCluster("vc-a", 1);

        assertThat(vcNames).containsExactly("vc-a");
        assertThat(leaderIds).containsExactly(OptionalInt.of(1));
    }

    @Test
    void shouldFirePartitionAssignmentListener() {
        var vcNames = new ArrayList<String>();
        var namespaces = new ArrayList<String>();
        var assignments = new ArrayList<PartitionAssignment>();
        cluster2.addListener(new ProxyClusterListener() {
            @Override
            public void onPartitionAssignmentChanged(
                                                     String virtualClusterName,
                                                     String namespace,
                                                     PartitionAssignment assignment) {
                vcNames.add(virtualClusterName);
                namespaces.add(namespace);
                assignments.add(assignment);
            }
        });

        var assignment = SimplePartitionAssignment.rangeAssignment(4, List.of(1, 2));
        state.setPartitionAssignment("vc-a", "__consumer_offsets", assignment);

        assertThat(vcNames).containsExactly("vc-a");
        assertThat(namespaces).containsExactly("__consumer_offsets");
        assertThat(assignments).containsExactly(assignment);
    }

    @Test
    void shouldRemoveListener() {
        var count = new int[]{ 0 };
        ProxyClusterListener listener = new ProxyClusterListener() {
            @Override
            public void onMembershipChanged(Set<ProxyClusterMember> members) {
                count[0]++;
            }
        };
        cluster1.addListener(listener);

        state.setMembers(Set.of(MEMBER_1));
        assertThat(count[0]).isEqualTo(1);

        cluster1.removeListener(listener);

        state.setMembers(Set.of(MEMBER_1, MEMBER_2));
        assertThat(count[0]).isEqualTo(1);
    }

    @Test
    void shouldCreateStandaloneCluster() {
        var standalone = InMemoryProxyCluster.standalone(1, "localhost", 9092, Transport.TCP);

        assertThat(standalone.localNodeId()).isEqualTo(1);
        assertThat(standalone.members()).hasSize(1);
        assertThat(standalone.localMember().nodeId()).isEqualTo(1);
        assertThat(standalone.localMember().rackId()).isNull();
        assertThat(standalone.localMember().endpoints()).hasSize(1);

        var endpoint = standalone.localMember().endpoints().get(0);
        assertThat(endpoint.host()).isEqualTo("localhost");
        assertThat(endpoint.port()).isEqualTo(9092);
        assertThat(endpoint.transport()).isEqualTo(Transport.TCP);
    }

    @Test
    void shouldRejectNonPositiveLocalNodeId() {
        assertThatThrownBy(() -> new InMemoryProxyCluster(0, state))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldRejectNullState() {
        assertThatThrownBy(() -> new InMemoryProxyCluster(1, null))
                .isInstanceOf(NullPointerException.class);
    }
}
