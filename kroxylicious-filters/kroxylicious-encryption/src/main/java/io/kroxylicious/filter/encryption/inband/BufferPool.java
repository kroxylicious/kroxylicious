/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.inband;

import java.nio.ByteBuffer;

public interface BufferPool {

    ByteBuffer acquire(int size);

    void release(ByteBuffer buffer);

    static BufferPool allocating() {
        return new BufferPool() {
            @Override
            public ByteBuffer acquire(int size) {
                return ByteBuffer.allocate(size);
            }

            @Override
            public void release(ByteBuffer buffer) {
                // this impl doesn't pool, so leave it to the GC to tidy up
            }
        };
    }
}
