/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.routing;

import java.util.concurrent.atomic.AtomicInteger;

import io.kroxylicious.proxy.tag.ThreadSafe;

/**
 * Allocates integer correlation IDs from a bounded circular range {@code [minInc, maxExc)}.
 * <p>
 * Each call to {@link #allocateId()} returns the next available ID. When the allocated
 * value reaches {@code maxExc - 1}, subsequent allocations wrap back to {@code minInc}.
 */
@ThreadSafe
class CorrelationIdAllocator {
    private final AtomicInteger nextRoutingCorrelationId;
    private final int minInc;
    private final int maxExc;

    /**
     * Creates an allocator for the range {@code [minInc, maxExc)}, starting at {@code minInc}.
     *
     * @param minInc lower bound, inclusive
     * @param maxExc upper bound, exclusive
     * @throws IllegalArgumentException if {@code minInc >= maxExc}
     */
    CorrelationIdAllocator(int minInc, int maxExc) {
        this(minInc, maxExc, minInc);
    }

    /**
     * Creates an allocator for the range {@code [minInc, maxExc)}, starting at {@code initial}.
     *
     * @param minInc  lower bound, inclusive
     * @param maxExc  upper bound, exclusive
     * @param initial first value to allocate; must be in {@code [minInc, maxExc)}
     * @throws IllegalArgumentException if {@code minInc >= maxExc}, or {@code initial} is outside {@code [minInc, maxExc)}
     */
    CorrelationIdAllocator(int minInc, int maxExc, int initial) {
        this(new AtomicInteger(initial), minInc, maxExc);
    }

    private CorrelationIdAllocator(AtomicInteger nextRoutingCorrelationId, int minInc, int maxExc) {
        if (minInc >= maxExc) {
            throw new IllegalArgumentException("Invalid min/max values: " + minInc + "/" + maxExc);
        }
        int initial = nextRoutingCorrelationId.get();
        if (initial < minInc) {
            throw new IllegalArgumentException("start must be greater than or equal to " + minInc);
        }
        if (initial >= maxExc) {
            throw new IllegalArgumentException("start must be less than " + maxExc);
        }
        this.nextRoutingCorrelationId = new AtomicInteger(initial);
        this.minInc = minInc;
        this.maxExc = maxExc;
    }

    /**
     * Returns the next correlation ID in the range, wrapping to {@code minInc} after {@code maxExc - 1}.
     *
     * @return the allocated correlation ID
     */
    int allocateId() {
        return nextRoutingCorrelationId.getAndUpdate(operand -> operand >= maxExc - 1 ? minInc : operand + 1);
    }

}
