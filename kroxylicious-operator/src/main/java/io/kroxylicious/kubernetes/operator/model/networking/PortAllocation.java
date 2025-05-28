/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.model.networking;

import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public record PortAllocation(List<PortRange> ranges) {

    private static final PortAllocation EMPTY = new PortAllocation(List.of());

    public PortAllocation {
        Objects.requireNonNull(ranges);
    }

    public IntStream getPorts() {
        return ranges.stream().flatMapToInt(r -> IntStream.rangeClosed(r.firstPort(), r.lastPort()));
    }

    public PortAllocation concat(PortAllocation other) {
        if (other.isEmpty()) {
            return this;
        }
        return new PortAllocation(Stream.concat(ranges.stream(), other.ranges.stream()).sorted(Comparator.comparing(PortRange::firstPort)).toList());
    }

    public static PortAllocation empty() {
        return EMPTY;
    }

    public static PortAllocation range(int firstPort, int lastPort) {
        return new PortAllocation(List.of(new PortRange(firstPort, lastPort)));
    }

    public int portCount() {
        if (ranges.isEmpty()) {
            return 0;
        }
        return ranges.stream().mapToInt(PortRange::portCount).sum();
    }

    public int firstPort() {
        return getPorts().findFirst().orElseThrow(() -> new IllegalStateException("empty allocation doesn't have a first ports"));
    }

    public boolean isEmpty() {
        return ranges.isEmpty();
    }
}
