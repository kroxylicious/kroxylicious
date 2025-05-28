/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.model.networking.allocation;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import io.kroxylicious.kubernetes.operator.model.networking.PortAllocation;
import io.kroxylicious.kubernetes.operator.model.networking.PortRange;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

public sealed abstract class PortRangeAllocator permits PortRangeAllocator.Terminal, PortRangeAllocator.Range {

    protected @Nullable PortRangeAllocator next;
    protected @Nullable PortRangeAllocator previous;

    private enum AllocationState {
        RESERVED,
        UNALLOCATED,
        ALLOCATED
    }

    // marks the start or end of the entire interval
    public static final class Terminal extends PortRangeAllocator {

        private Terminal(PortRangeAllocator previous, PortRangeAllocator next) {
            if (previous == null && next == null) {
                throw new IllegalArgumentException("Both previous and next can't be null");
            }
            if (previous != null && next != null) {
                throw new IllegalArgumentException("Both previous and next can't be set, the terminal should be for one end of the chain");
            }
            this.previous = previous;
            this.next = next;
        }

        @Override
        public void reserve(PortRange range, String clusterName, String ingressName) {
            if (canReserve(range, clusterName, ingressName)) {
                PortRangeAllocator current = next;
                while (current != null) {
                    current.reserve(range, clusterName, ingressName);
                    current = current.next;
                }
            }
            else {
                throw new IllegalStateException("Cannot reserve " + range);
            }
        }

        @Override
        public PortAllocation allocate(String clusterName, String ingressName, int requiredPorts) {
            PortAllocation portAllocation = allocateReserved(clusterName, ingressName, requiredPorts);
            int remaining = requiredPorts - portAllocation.portCount();
            PortRangeAllocator current = next;
            while (current != null && remaining > 0) {
                PortAllocation portAllocation1 = current.allocate(clusterName, ingressName, remaining);
                remaining -= portAllocation1.portCount();
                current = current.next;
                portAllocation = portAllocation.concat(portAllocation1);
            }
            return portAllocation;
        }

        @Override
        PortAllocation allocateReserved(String clusterName, String ingressName, int requiredPorts) {
            PortAllocation portAllocation = PortAllocation.empty();
            int remaining = requiredPorts;
            PortRangeAllocator current = next;
            while (current != null && remaining > 0) {
                PortAllocation portAllocation1 = current.allocateReserved(clusterName, ingressName, remaining);
                remaining -= portAllocation1.portCount();
                current = current.next;
                portAllocation = portAllocation.concat(portAllocation1);
            }
            return portAllocation;
        }

        @Override
        public List<Allocation> allocations() {
            ArrayList<Allocation> allocations = new ArrayList<>();
            PortRangeAllocator current = next;
            while (current != null) {
                allocations.addAll(current.allocations());
                current = current.next;
            }
            return allocations;
        }

        @Override
        boolean canReserve(PortRange range, String clusterName, String ingressName) {
            boolean canReserve = true;
            PortRangeAllocator current = next;
            while (current != null) {
                canReserve = canReserve && current.canReserve(range, clusterName, ingressName);
                current = current.next;
            }
            return canReserve;
        }

        @Override
        public String toString() {
            PortRangeAllocator toPrint = next;
            StringBuilder stringBuilder = new StringBuilder();
            while (toPrint != null) {
                stringBuilder.append(toPrint);
                toPrint = toPrint.next;
            }
            return stringBuilder.toString();
        }

        public static PortRangeAllocator end(PortRangeAllocator next) {
            Terminal terminal = new Terminal(next, null);
            next.next = terminal;
            return terminal;
        }

        public static PortRangeAllocator start(PortRangeAllocator next) {
            Terminal terminal = new Terminal(null, next);
            next.previous = terminal;
            return terminal;
        }
    }

    public static final class Range extends PortRangeAllocator {
        private final PortRange range;
        private final AllocationState allocationState;
        private final @Nullable String clusterName;
        private final @Nullable String ingressName;

        private Range(PortRange range, AllocationState allocationState) {
            this(range, allocationState, null, null);
        }

        private Range(PortRange range, AllocationState allocationState, @Nullable String clusterName, @Nullable String ingressName) {
            this.range = range;
            this.allocationState = allocationState;
            this.clusterName = clusterName;
            this.ingressName = ingressName;
        }

        private Range(int firstPort, int lastPort, AllocationState allocationState, @Nullable String clusterName, @Nullable String ingressName) {
            this(new PortRange(firstPort, lastPort), allocationState, clusterName, ingressName);
        }

        void split(PortRange range, AllocationState newAllocationState, @Nullable String clusterName, @Nullable String ingressName) {
            if (range.overlaps(this.range)) {
                if (this.range.equals(range) || range.contains(this.range)) {
                    linkAndReplaceSelf(new Range(this.range.firstPort(), this.range.lastPort(), newAllocationState, clusterName, ingressName));
                }
                else if (this.range.firstPort() < range.firstPort() && this.range.lastPort() > range.lastPort()) {
                    // the desired range is contained by this range with ports either side
                    linkAndReplaceSelf(new Range(this.range.firstPort(), range.firstPort() - 1, this.allocationState, clusterName, ingressName),
                            new Range(range.firstPort(), range.lastPort(), newAllocationState, clusterName, ingressName),
                            new Range(range.lastPort() + 1, this.range.lastPort(), this.allocationState, this.clusterName, this.ingressName));
                }
                else if (this.range.firstPort() < range.firstPort()) {
                    // this range contains the start of the desired range
                    linkAndReplaceSelf(new Range(this.range.firstPort(), range.lastPort(), this.allocationState, this.clusterName, this.ingressName),
                            new Range(range.lastPort() + 1, this.range.lastPort(), newAllocationState, clusterName, ingressName));
                }
                else {
                    // this range contains the end of the desired range
                    linkAndReplaceSelf(new Range(this.range.firstPort(), range.lastPort(), newAllocationState, clusterName, ingressName),
                            new Range(range.lastPort() + 1, this.range.lastPort(), this.allocationState, this.clusterName, this.ingressName));
                }
            }
        }

        @Override
        public void reserve(PortRange range, String clusterName, String ingressName) {
            if (range.overlaps(this.range)) {
                if (this.allocationState == AllocationState.UNALLOCATED) {
                    split(range, AllocationState.RESERVED, clusterName, ingressName);
                }
                else {
                    throw new IllegalStateException("cannot reserve " + range + " for cluster " + clusterName + " and ingress " + ingressName);
                }
            }
        }

        @Override
        public PortAllocation allocate(String clusterName, String ingressName, int requiredPorts) {
            if (requiredPorts <= 0) {
                return PortAllocation.empty();
            }
            else if (range.overlaps(this.range) &&
                    this.allocationState == AllocationState.UNALLOCATED) {
                PortRange allocated = doAllocate(clusterName, ingressName, requiredPorts);
                return new PortAllocation(List.of(allocated));
            }
            else {
                return PortAllocation.empty();
            }
        }

        @NonNull
        private PortRange doAllocate(String clusterName, String ingressName, int requiredPorts) {
            int allocatable = range.portCount();
            int toAllocate = Math.min(allocatable, requiredPorts);
            PortRange allocateRange = new PortRange(this.range.firstPort(), this.range.firstPort() + toAllocate - 1);
            split(allocateRange, AllocationState.ALLOCATED, clusterName, ingressName);
            return allocateRange;
        }

        @Override
        PortAllocation allocateReserved(String clusterName, String ingressName, int requiredPorts) {
            if (requiredPorts <= 0) {
                return PortAllocation.empty();
            }
            else if (this.allocationState == AllocationState.RESERVED
                    && range.overlaps(this.range)
                    && Objects.requireNonNull(this.clusterName).equals(clusterName)
                    && Objects.requireNonNull(this.ingressName).equals(ingressName)) {

                PortRange allocated = doAllocate(clusterName, ingressName, requiredPorts);
                return new PortAllocation(List.of(allocated));
            }
            else {
                return PortAllocation.empty();
            }
        }

        @Override
        public List<Allocation> allocations() {
            if (this.allocationState != AllocationState.ALLOCATED) {
                return List.of();
            }
            else {
                return List.of(new Allocation(this.clusterName, this.ingressName, this.range));
            }
        }

        @Override
        boolean canReserve(PortRange range, String clusterName, String ingressName) {
            if (this.range.overlaps(range)) {
                return allocationState == AllocationState.UNALLOCATED;
            }
            else {
                return true;
            }
        }

        void linkAndReplaceSelf(Range... ranges) {
            for (int i = 0; i < ranges.length; i++) {
                Range r = ranges[i];
                if (i == 0) {
                    r.previous = this.previous;
                    Objects.requireNonNull(this.previous).next = r;
                }
                else {
                    r.previous = ranges[i - 1];
                    ranges[i - 1].next = r;
                }
                if (i == ranges.length - 1) {
                    r.next = this.next;
                    Objects.requireNonNull(this.next).previous = r;
                }
            }
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            if (!super.equals(o)) {
                return false;
            }
            Range range1 = (Range) o;
            return Objects.equals(range, range1.range) && allocationState == range1.allocationState && Objects.equals(clusterName, range1.clusterName) && Objects.equals(
                    ingressName, range1.ingressName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), range, allocationState, clusterName, ingressName);
        }

        @Override
        public String toString() {
            return "Range{" +
                    "range=" + range +
                    ", type=" + allocationState +
                    ", clusterName='" + clusterName + '\'' +
                    ", ingressName='" + ingressName + '\'' +
                    '}';
        }
    }

    public void reserve(Allocation allocation) {
        reserve(allocation.range(), allocation.clusterName(), allocation.ingressName());
    }

    public abstract void reserve(PortRange range, String clusterName, String ingressName);

    public abstract PortAllocation allocate(String clusterName, String ingressName, int requiredPorts);

    abstract PortAllocation allocateReserved(String clusterName, String ingressName, int requiredPorts);

    public abstract List<Allocation> allocations();

    abstract boolean canReserve(PortRange range, String clusterName, String ingressName);

    public static PortRangeAllocator createUnallocated(PortRange range) {
        PortRangeAllocator unallocated = new Range(range, AllocationState.UNALLOCATED);
        PortRangeAllocator start = Terminal.start(unallocated);
        Terminal.end(unallocated);
        return start;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PortRangeAllocator that = (PortRangeAllocator) o;
        return Objects.equals(next, that.next) && Objects.equals(previous, that.previous);
    }

    @Override
    public int hashCode() {
        return Objects.hash(next, previous);
    }
}
