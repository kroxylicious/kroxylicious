/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.inband;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class BufferPoolTest {

    @Test
    void testAllocating() {
        BufferPool allocating = BufferPool.allocating();
        var bb = allocating.acquire(123);
        assertEquals(123, bb.capacity());
        assertEquals(0, bb.position());
        allocating.release(bb);
    }
}