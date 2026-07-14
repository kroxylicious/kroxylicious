/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.assertj.core.api.Assertions.assertThat;

class CorrelationIdSpaceTest {

    @Test
    public void createRouterAllocator() {
        CorrelationIdAllocator allocator = CorrelationIdSpace.createRouterAllocator();
        assertThat(allocator.minInc()).isEqualTo(Integer.MIN_VALUE / 2);
        assertThat(allocator.maxExc()).isEqualTo(0);
    }

    @Test
    public void createRouterAllocatorDoesNotContainOutOfBandId() {
        CorrelationIdAllocator allocator = CorrelationIdSpace.createRouterAllocator();
        assertThat(allocator.inRange(CorrelationIdSpace.RESERVED_OUT_OF_BAND_CORRELATION_ID)).isFalse();
    }

    @CsvSource({
            "1, false",
            Integer.MAX_VALUE + ", false",
            "0, false",
            "-1, true",
            CorrelationIdSpace.RESERVED_OUT_OF_BAND_CORRELATION_ID + ", false",
            Integer.MIN_VALUE / 2 + ", true"
    })
    @ParameterizedTest
    public void isRoutingCorrelationId(int candidate, boolean expected) {
        assertThat(CorrelationIdSpace.isRoutingCorrelationId(candidate)).isEqualTo(expected);
    }

    @Test
    public void routerSpaceDoesNotConflictWithOutOfBandId() {
        assertThat(CorrelationIdSpace.RESERVED_OUT_OF_BAND_CORRELATION_ID)
                .isLessThan(CorrelationIdSpace.RESERVED_ROUTING_ID_RANGE_START_INC)
                .isLessThan(CorrelationIdSpace.RESERVED_ROUTING_ID_RANGE_END_EXC);
    }

}