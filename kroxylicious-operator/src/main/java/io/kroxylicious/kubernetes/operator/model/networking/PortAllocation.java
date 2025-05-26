/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.model.networking;

import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public record PortAllocation(List<PortRange> ranges) {

    public PortAllocation {
        Objects.requireNonNull(ranges);
    }

    public IntStream getPorts() {
        return ranges.stream().flatMapToInt(r -> IntStream.rangeClosed(r.firstPort(), r.lastPort()));
    }

    public PortAllocation concat(PortAllocation other) {
        return new PortAllocation(Stream.concat(ranges.stream(), other.ranges.stream()).toList());
    }

    public static PortAllocation empty() {
        return new PortAllocation(List.of());
    }

    public static PortAllocation range(int firstPort, int lastPort) {
        return new PortAllocation(List.of(new PortRange(firstPort, lastPort)));
    }

    public int portCount() {
        return ranges.stream().mapToInt(PortRange::portCount).sum();
    }

    public int firstPort() {
        return getPorts().findFirst().orElseThrow(() -> new IllegalStateException("empty allocation doesn't have a first ports"));
    }
}
