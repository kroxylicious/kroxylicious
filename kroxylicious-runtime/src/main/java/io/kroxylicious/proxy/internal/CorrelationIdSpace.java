/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

/**
 * Divides the correlation id space into ranges reserved for different internal purposes.
 */
public class CorrelationIdSpace {

    /** Correlation id reserved for out-of-band internal requests; outside the routing range to avoid collisions. */
    public static final int RESERVED_OUT_OF_BAND_CORRELATION_ID = Integer.MIN_VALUE;
    /** Inclusive start of the correlation id range reserved for routing ({@code Integer.MIN_VALUE / 2}). */
    public static final int RESERVED_ROUTING_ID_RANGE_START_INC = Integer.MIN_VALUE / 2;
    /** Exclusive end of the correlation id range reserved for routing. */
    public static final int RESERVED_ROUTING_ID_RANGE_END_EXC = 0;

    private CorrelationIdSpace() {
        // empty private constructor prevents instantiation
    }

    static {
        if (isRoutingCorrelationId(RESERVED_OUT_OF_BAND_CORRELATION_ID)) {
            throw new IllegalStateException("CorrelationId space is reserved for routing ids");
        }
    }

    /**
     * Determines whether the given correlation id falls within the range reserved for routing.
     * @param correlationId the correlation id to test
     * @return {@code true} if the id is in the reserved routing range
     */
    public static boolean isRoutingCorrelationId(int correlationId) {
        return correlationId >= RESERVED_ROUTING_ID_RANGE_START_INC && correlationId < RESERVED_ROUTING_ID_RANGE_END_EXC;
    }

    /**
     * Creates an allocator that hands out correlation ids from the reserved routing range.
     * @return a new allocator over the routing correlation id range
     */
    public static CorrelationIdAllocator createRouterAllocator() {
        return new CorrelationIdAllocator(RESERVED_ROUTING_ID_RANGE_START_INC, RESERVED_ROUTING_ID_RANGE_END_EXC);
    }
}
