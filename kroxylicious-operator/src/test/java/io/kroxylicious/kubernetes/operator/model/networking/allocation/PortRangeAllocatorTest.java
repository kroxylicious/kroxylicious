/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.model.networking.allocation;

import java.util.List;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;

import io.kroxylicious.kubernetes.operator.model.networking.PortAllocation;
import io.kroxylicious.kubernetes.operator.model.networking.PortRange;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PortRangeAllocatorTest {

    @Test
    void testAllocation() {
        PortRangeAllocator unallocated = PortRangeAllocator.createUnallocated(new PortRange(1, 10));
        PortAllocation allocate = unallocated.allocate("a", "b", 5);
        assertThat(allocate.ranges()).containsExactly(new PortRange(1, 5));
    }

    @Test
    void testGetAllocations() {
        PortRangeAllocator unallocated = PortRangeAllocator.createUnallocated(new PortRange(1, 10));
        unallocated.allocate("a", "b", 5);
        assertThat(unallocated.allocations()).containsExactly(new Allocation("a", "b", new PortRange(1, 5)));
    }

    @Test
    void testGetMultipleAllocations() {
        PortRangeAllocator unallocated = PortRangeAllocator.createUnallocated(new PortRange(1, 10));
        unallocated.allocate("a", "b", 2);
        unallocated.allocate("a", "b", 2);
        assertThat(unallocated.allocations()).containsExactly(new Allocation("a", "b", new PortRange(1, 2)),
                new Allocation("a", "b", new PortRange(3, 4)));
    }

    @Test
    void testGetManyAllocations() {
        PortRangeAllocator unallocated = PortRangeAllocator.createUnallocated(new PortRange(1, 10000));
        for (int i = 0; i < 10000; i++) {
            unallocated.allocate("a", "b", 1);
        }
        List<Allocation> expected = IntStream.rangeClosed(1, 10000).mapToObj(i -> new Allocation("a", "b", new PortRange(i, i))).toList();
        assertThat(unallocated.allocations()).containsExactlyElementsOf(expected);
    }

    @Test
    void testAllocationsAreDiscrete() {
        PortRangeAllocator unallocated = PortRangeAllocator.createUnallocated(new PortRange(1, 10));
        PortAllocation allocate = unallocated.allocate("a", "b", 5);
        assertThat(allocate.ranges()).containsExactly(new PortRange(1, 5));
        PortAllocation allocate2 = unallocated.allocate("a", "b", 5);
        assertThat(allocate2.ranges()).containsExactly(new PortRange(6, 10));
    }

    @Test
    void testOnlyAllocatesAvailablePorts() {
        PortRangeAllocator unallocated = PortRangeAllocator.createUnallocated(new PortRange(1, 5));
        // request more ports than are available
        PortAllocation allocate = unallocated.allocate("a", "b", 6);
        assertThat(allocate.ranges()).containsExactly(new PortRange(1, 5));

        PortAllocation allocate2 = unallocated.allocate("a", "b", 6);
        assertThat(allocate2.ranges()).isEmpty();
    }

    @Test
    void testMultipleReservationsPreferred() {
        PortRangeAllocator unallocated = PortRangeAllocator.createUnallocated(new PortRange(1, 6));
        unallocated.reserve(new PortRange(2, 2), "a", "b");
        unallocated.reserve(new PortRange(4, 5), "a", "b");
        // request more ports than are available
        PortAllocation allocate = unallocated.allocate("a", "b", 4);
        assertThat(allocate.ranges()).containsExactly(new PortRange(1, 1), new PortRange(2, 2), new PortRange(4, 5));
    }

    @Test
    void testCannotReserveRangeTwice_Exact() {
        PortRangeAllocator unallocated = PortRangeAllocator.createUnallocated(new PortRange(1, 6));
        unallocated.reserve(new PortRange(2, 2), "a", "b");
        assertThatThrownBy(() -> {
            unallocated.reserve(new PortRange(2, 2), "a", "b");
        }).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void testCannotReserveRangeTwice_ContainsPriorReservation() {
        PortRangeAllocator unallocated = PortRangeAllocator.createUnallocated(new PortRange(1, 6));
        unallocated.reserve(new PortRange(2, 2), "a", "b");
        assertThatThrownBy(() -> {
            unallocated.reserve(new PortRange(1, 3), "a", "b");
        }).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void testCannotReserveRangeTwice_ContainedWithinPriorReservation() {
        PortRangeAllocator unallocated = PortRangeAllocator.createUnallocated(new PortRange(1, 6));
        unallocated.reserve(new PortRange(2, 4), "a", "b");
        assertThatThrownBy(() -> {
            unallocated.reserve(new PortRange(3, 3), "a", "b");
        }).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void testCannotReserveRangeTwice_OverlapsHighEndOfPriorReservation() {
        PortRangeAllocator unallocated = PortRangeAllocator.createUnallocated(new PortRange(1, 6));
        unallocated.reserve(new PortRange(2, 4), "a", "b");
        assertThatThrownBy(() -> {
            unallocated.reserve(new PortRange(4, 5), "a", "b");
        }).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void testCannotReserveRangeTwice_OverlapsLowEndOfPriorReservation() {
        PortRangeAllocator unallocated = PortRangeAllocator.createUnallocated(new PortRange(1, 6));
        unallocated.reserve(new PortRange(2, 4), "a", "b");
        assertThatThrownBy(() -> {
            unallocated.reserve(new PortRange(1, 2), "a", "b");
        }).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void testCannotReserveRangeTwice_ContainsPriorReservationFromAnotherCluster() {
        PortRangeAllocator unallocated = PortRangeAllocator.createUnallocated(new PortRange(1, 6));
        unallocated.reserve(new PortRange(2, 2), "a", "b");
        assertThatThrownBy(() -> {
            unallocated.reserve(new PortRange(2, 2), "b", "b");
        }).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void testCannotReserveAllocatedRange() {
        PortRangeAllocator unallocated = PortRangeAllocator.createUnallocated(new PortRange(1, 6));
        unallocated.allocate("a", "b", 2);
        assertThatThrownBy(() -> {
            unallocated.reserve(new PortRange(1, 2), "a", "b");
        }).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void testCanReserveUnallocatedRange() {
        PortRangeAllocator unallocated = PortRangeAllocator.createUnallocated(new PortRange(1, 6));
        unallocated.allocate("a", "b", 2);
        unallocated.reserve(new PortRange(3, 4), "a", "b");
    }

    @Test
    void testMultipleAllocationsForMultipleReservations() {
        PortRangeAllocator unallocated = PortRangeAllocator.createUnallocated(new PortRange(1, 6));
        unallocated.reserve(new PortRange(2, 2), "a", "b");
        unallocated.reserve(new PortRange(4, 5), "a", "b");
        // request more ports than are available
        PortAllocation allocate = unallocated.allocate("a", "b", 1);
        assertThat(allocate.ranges()).containsExactly(new PortRange(2, 2));
        PortAllocation allocate2 = unallocated.allocate("a", "b", 1);
        assertThat(allocate2.ranges()).containsExactly(new PortRange(4, 4));
        PortAllocation allocate3 = unallocated.allocate("a", "b", 1);
        assertThat(allocate3.ranges()).containsExactly(new PortRange(5, 5));
        PortAllocation allocate4 = unallocated.allocate("a", "b", 1);
        assertThat(allocate4.ranges()).containsExactly(new PortRange(1, 1));
    }

    @Test
    void testReservationPreferredAtAllocationTime() {
        PortRangeAllocator unallocated = PortRangeAllocator.createUnallocated(new PortRange(1, 10));
        unallocated.reserve(new PortRange(4, 5), "a", "b");
        PortAllocation allocate = unallocated.allocate("a", "b", 2);
        assertThat(allocate.ranges()).containsExactly(new PortRange(4, 5));
    }

    @Test
    void testReservationCrossingAnInterval() {
        PortRangeAllocator unallocated = PortRangeAllocator.createUnallocated(new PortRange(1, 10));
        unallocated.reserve(new PortRange(4, 5), "a", "b");
        PortAllocation allocate = unallocated.allocate("a", "b", 2);
        assertThat(allocate.ranges()).containsExactly(new PortRange(4, 5));
    }

    @Test
    void testReservationUsesFreePortsAfterReservationExhausted() {
        PortRangeAllocator unallocated = PortRangeAllocator.createUnallocated(new PortRange(1, 10));
        unallocated.reserve(new PortRange(4, 5), "a", "b");
        // requires more ports than are reserved
        PortAllocation allocate = unallocated.allocate("a", "b", 3);
        assertThat(allocate.ranges()).containsExactly(new PortRange(1, 1), new PortRange(4, 5));
    }

    @Test
    void testReservationOnlyUsedForAppropriateClusterIngress() {
        PortRangeAllocator unallocated = PortRangeAllocator.createUnallocated(new PortRange(1, 10));
        unallocated.reserve(new PortRange(2, 3), "other", "other");
        unallocated.reserve(new PortRange(4, 5), "a", "b");

        PortAllocation allocate = unallocated.allocate("a", "b", 3);
        assertThat(allocate.ranges()).containsExactly(new PortRange(1, 1), new PortRange(4, 5));

        PortAllocation allocate2 = unallocated.allocate("other", "other", 3);
        assertThat(allocate2.ranges()).containsExactly(new PortRange(2, 3), new PortRange(6, 6));
    }

}
