/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.frame;

import io.netty.buffer.ByteBuf;

public class OpaqueResponseFrame extends OpaqueFrame implements ResponseFrame {

    private final RequestResponseState requestResponseState;

    public OpaqueResponseFrame(ByteBuf buf, int correlationId, int length, RequestResponseState requestResponseState) {
        super(buf, correlationId, length);
        this.requestResponseState = requestResponseState;
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

    @Override
    public RequestResponseState requestResponseState() {
        return requestResponseState;
    }
}
