/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.frame;

import io.netty.buffer.ByteBuf;

/**
 * Used to represent Kafka responses that the proxy does not need to decode.
 */
public class OpaqueResponseFrame extends OpaqueFrame implements ResponseFrame {
    /**
     * Creates an opaque response.
     *
     * @param buf The message buffer (excluding the frame size)
     * @param correlationId correlation id
     * @param length length of the response
     */
    public OpaqueResponseFrame(ByteBuf buf, int correlationId, int length) {
        super(buf, correlationId, length);
    }

    @Override
    public String toString() {
        int index = buf.readerIndex();
        try {
            var correlationId = buf.readInt();
            return getClass().getSimpleName() + "(" +
                    "length=" + length +
                    ", correlationId=" + correlationId +
                    ", buf=" + buf +
                    ')';
        }
        finally {
            buf.readerIndex(index);
        }
    }
}
