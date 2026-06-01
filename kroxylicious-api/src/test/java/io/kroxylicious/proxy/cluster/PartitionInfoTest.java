/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.cluster;

import java.util.ArrayList;
import java.util.List;
import java.util.OptionalInt;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PartitionInfoTest {

    @Test
    void shouldCreateValidPartitionInfo() {
        var info = new PartitionInfo(0, 1, OptionalInt.of(42), List.of(1, 2, 3), List.of(1, 2));

        assertThat(info.partitionIndex()).isZero();
        assertThat(info.leaderEpoch()).isEqualTo(1);
        assertThat(info.leader()).hasValue(42);
        assertThat(info.replicas()).containsExactly(1, 2, 3);
        assertThat(info.isr()).containsExactly(1, 2);
    }

    @Test
    void shouldAcceptEmptyLeader() {
        var info = new PartitionInfo(0, 0, OptionalInt.empty(), List.of(), List.of());

        assertThat(info.leader()).isEmpty();
    }

    @Test
    void shouldRejectNegativePartitionIndex() {
        assertThatThrownBy(() -> new PartitionInfo(-1, 0, OptionalInt.empty(), List.of(), List.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("partitionIndex must be non-negative");
    }

    @Test
    void shouldRejectNegativeLeaderEpoch() {
        assertThatThrownBy(() -> new PartitionInfo(0, -1, OptionalInt.empty(), List.of(), List.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("leaderEpoch must be non-negative");
    }

    @Test
    void shouldRejectNullLeader() {
        assertThatThrownBy(() -> new PartitionInfo(0, 0, null, List.of(), List.of()))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void shouldRejectNullReplicas() {
        assertThatThrownBy(() -> new PartitionInfo(0, 0, OptionalInt.empty(), null, List.of()))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void shouldRejectNullIsr() {
        assertThatThrownBy(() -> new PartitionInfo(0, 0, OptionalInt.empty(), List.of(), null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void shouldDefensivelyCopyReplicas() {
        var mutable = new ArrayList<>(List.of(1, 2));
        var info = new PartitionInfo(0, 0, OptionalInt.empty(), mutable, List.of());

        mutable.add(3);

        assertThat(info.replicas()).containsExactly(1, 2);
    }

    @Test
    void shouldDefensivelyCopyIsr() {
        var mutable = new ArrayList<>(List.of(1));
        var info = new PartitionInfo(0, 0, OptionalInt.empty(), List.of(), mutable);

        mutable.add(2);

        assertThat(info.isr()).containsExactly(1);
    }

    @Test
    void shouldReturnUnmodifiableReplicas() {
        var info = new PartitionInfo(0, 0, OptionalInt.empty(), List.of(1), List.of());

        assertThatThrownBy(() -> info.replicas().add(2))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void shouldReturnUnmodifiableIsr() {
        var info = new PartitionInfo(0, 0, OptionalInt.empty(), List.of(), List.of(1));

        assertThatThrownBy(() -> info.isr().add(2))
                .isInstanceOf(UnsupportedOperationException.class);
    }
}
