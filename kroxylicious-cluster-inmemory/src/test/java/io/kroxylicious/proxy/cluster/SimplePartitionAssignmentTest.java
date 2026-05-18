/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.cluster;

import java.util.List;
import java.util.OptionalInt;
import java.util.Set;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SimplePartitionAssignmentTest {

    @Test
    void shouldCreateFromExplicitPartitionInfo() {
        var p0 = new PartitionInfo(0, 1, OptionalInt.of(1), List.of(1), List.of(1));
        var p1 = new PartitionInfo(1, 1, OptionalInt.of(2), List.of(2), List.of(2));
        var assignment = new SimplePartitionAssignment(p0, p1);

        assertThat(assignment.partitionCount()).isEqualTo(2);
        assertThat(assignment.partitionInfo(0)).isEqualTo(p0);
        assertThat(assignment.partitionInfo(1)).isEqualTo(p1);
    }

    @Test
    void shouldRangeAssignToSingleNode() {
        var assignment = SimplePartitionAssignment.rangeAssignment(3, List.of(1));

        assertThat(assignment.partitionCount()).isEqualTo(3);
        for (int i = 0; i < 3; i++) {
            var info = assignment.partitionInfo(i);
            assertThat(info.partitionIndex()).isEqualTo(i);
            assertThat(info.leader()).hasValue(1);
            assertThat(info.replicas()).containsExactly(1);
            assertThat(info.isr()).containsExactly(1);
            assertThat(info.leaderEpoch()).isEqualTo(1);
        }
    }

    @Test
    void shouldRangeAssignToTwoNodes() {
        var assignment = SimplePartitionAssignment.rangeAssignment(4, List.of(1, 2));

        assertThat(assignment.partitionCount()).isEqualTo(4);
        assertThat(assignment.partitionInfo(0).leader()).hasValue(1);
        assertThat(assignment.partitionInfo(1).leader()).hasValue(1);
        assertThat(assignment.partitionInfo(2).leader()).hasValue(2);
        assertThat(assignment.partitionInfo(3).leader()).hasValue(2);
    }

    @Test
    void shouldRangeAssignUnevenPartitions() {
        var assignment = SimplePartitionAssignment.rangeAssignment(5, List.of(1, 2));

        assertThat(assignment.partitionCount()).isEqualTo(5);
        assertThat(assignment.partitionsLedBy(1)).hasSize(3);
        assertThat(assignment.partitionsLedBy(2)).hasSize(2);
    }

    @Test
    void shouldRangeAssignToThreeNodes() {
        var assignment = SimplePartitionAssignment.rangeAssignment(6, List.of(1, 2, 3));

        assertThat(assignment.partitionsLedBy(1)).isEqualTo(Set.of(0, 1));
        assertThat(assignment.partitionsLedBy(2)).isEqualTo(Set.of(2, 3));
        assertThat(assignment.partitionsLedBy(3)).isEqualTo(Set.of(4, 5));
    }

    @Test
    void shouldReturnEmptySetForUnknownNodeId() {
        var assignment = SimplePartitionAssignment.rangeAssignment(3, List.of(1));

        assertThat(assignment.partitionsLedBy(99)).isEmpty();
    }

    @Test
    void shouldHandleZeroPartitions() {
        var assignment = SimplePartitionAssignment.rangeAssignment(0, List.of(1));

        assertThat(assignment.partitionCount()).isZero();
        assertThat(assignment.partitionsLedBy(1)).isEmpty();
    }

    @Test
    void shouldPreserveLeaderEpoch() {
        var info = new PartitionInfo(0, 42, OptionalInt.of(1), List.of(1), List.of(1));
        var assignment = new SimplePartitionAssignment(info);

        assertThat(assignment.partitionInfo(0).leaderEpoch()).isEqualTo(42);
    }

    @Test
    void shouldHandleNoLeader() {
        var info = new PartitionInfo(0, 1, OptionalInt.empty(), List.of(1, 2), List.of());
        var assignment = new SimplePartitionAssignment(info);

        assertThat(assignment.partitionInfo(0).leader()).isEmpty();
        assertThat(assignment.partitionsLedBy(1)).isEmpty();
        assertThat(assignment.partitionsLedBy(2)).isEmpty();
    }

    @Test
    void shouldThrowOnOutOfBoundsPartitionIndex() {
        var assignment = SimplePartitionAssignment.rangeAssignment(3, List.of(1));

        assertThatThrownBy(() -> assignment.partitionInfo(-1))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> assignment.partitionInfo(3))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    void shouldRejectNegativePartitionCount() {
        assertThatThrownBy(() -> SimplePartitionAssignment.rangeAssignment(-1, List.of(1)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldRejectEmptyNodeIds() {
        assertThatThrownBy(() -> SimplePartitionAssignment.rangeAssignment(3, List.of()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldRejectNullNodeIds() {
        assertThatThrownBy(() -> SimplePartitionAssignment.rangeAssignment(3, null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void shouldDefensivelyCopyPartitionInfoLists() {
        var replicas = new java.util.ArrayList<>(List.of(1, 2));
        var isr = new java.util.ArrayList<>(List.of(1));
        var info = new PartitionInfo(0, 1, OptionalInt.of(1), replicas, isr);

        replicas.add(3);
        isr.add(2);

        assertThat(info.replicas()).containsExactly(1, 2);
        assertThat(info.isr()).containsExactly(1);
    }

    @Test
    void shouldReturnUnmodifiablePartitionInfoLists() {
        var info = new PartitionInfo(0, 1, OptionalInt.of(1), List.of(1), List.of(1));

        assertThatThrownBy(() -> info.replicas().add(2))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> info.isr().add(2))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void shouldRejectNegativePartitionIndex() {
        assertThatThrownBy(() -> new PartitionInfo(-1, 1, OptionalInt.of(1), List.of(1), List.of(1)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldRejectNegativeLeaderEpoch() {
        assertThatThrownBy(() -> new PartitionInfo(0, -1, OptionalInt.of(1), List.of(1), List.of(1)))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
