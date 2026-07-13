/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

// A class that divides the correlation id space into ranges reserved for different internal purposes
public class CorrelationIdSpace {

    private CorrelationIdSpace() {

    }

    // use a correlation id outside the Routing range (Integer.MIN_VALUE/2, 0] to avoid collisions
    public static final int RESERVED_OUT_OF_BAND_CORRELATION_ID = Integer.MIN_VALUE;
}
