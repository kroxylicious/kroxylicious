/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

// A class that divides the correlation id space into ranges reserved for different internal purposes
public class CorrelationIdSpace {

    // use a correlation id outside the routing range [Integer.MIN_VALUE/2, 0) to avoid collisions
    public static final int RESERVED_OUT_OF_BAND_CORRELATION_ID = Integer.MIN_VALUE;
    public static final int RESERVED_ROUTING_ID_RANGE_START_INC = Integer.MIN_VALUE / 2;
    public static final int RESERVED_ROUTING_ID_RANGE_END_EXC = 0;

    private CorrelationIdSpace() {
        // empty private constructor prevents instantiation
    }

    static {
        if (isRoutingCorrelationId(RESERVED_OUT_OF_BAND_CORRELATION_ID)) {
            throw new IllegalStateException("CorrelationId space is reserved for routing ids");
        }
    }

    public static boolean isRoutingCorrelationId(int correlationId) {
        return correlationId >= RESERVED_ROUTING_ID_RANGE_START_INC && correlationId < RESERVED_ROUTING_ID_RANGE_END_EXC;
    }

    public static CorrelationIdAllocator createRouterAllocator() {
        return new CorrelationIdAllocator(RESERVED_ROUTING_ID_RANGE_START_INC, RESERVED_ROUTING_ID_RANGE_END_EXC);
    }
}
