/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.frame;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;

import io.kroxylicious.proxy.tag.VisibleForTesting;

/**
 * A frame in the Kafka protocol which has not been decoded.
 * The wrapped buffer <strong>does not</strong> include the frame size prefix.
 */
public abstract class OpaqueFrame implements Frame {

    private static final Logger LOGGER = LoggerFactory.getLogger(OpaqueFrame.class);

    /**
     * Number of bytes required for storing the frame length.
     */
    private static final int FRAME_SIZE_LENGTH = Integer.BYTES;

    protected final int length;
    protected final int correlationId;
    /** The message buffer excluding the frame size, including the header and body. */
    protected final ByteBuf buf;

    /**
     * @param buf The message buffer (excluding the frame size)
     * @param correlationId The correlation id
     * @param length The length of the frame within {@code buf}.
     */
    OpaqueFrame(ByteBuf buf, int correlationId, int length) {
        this.length = length;
        this.correlationId = correlationId;
        this.buf = buf.asReadOnly();
        if (buf.readableBytes() != length) {
            throw new AssertionError("readable: " + buf.readableBytes() + " length: " + length);
        }
    }

    @Override
    public int correlationId() {
        return correlationId;
    }

    @Override
    public int estimateEncodedSize() {
        return FRAME_SIZE_LENGTH + length;
    }

    @Override
    public void encode(ByteBufAccessor out) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(
                    "Writing {} with 4 byte length ({}) plus {} bytes from buffer {} to {}",
                    getClass().getSimpleName(),
                    length,
                    buf.readableBytes(),
                    buf,
                    out
            );
        }
        out.ensureWritable(estimateEncodedSize());
        out.writeInt(length);
        out.writeBytes(buf, length);
        buf.release();
    }

    @VisibleForTesting
    public ByteBuf buf() {
        return buf;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName()
               + "("
               +
               "length="
               + length
               +
               ", buf="
               + buf
               +
               ')';
    }
}
