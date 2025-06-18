/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.frame;

/**
 * A frame in the Kafka protocol, which may or may not be fully decoded.
 */
public interface Frame {

    /**
     * Number of bytes required for storing the frame length.
     */
    int FRAME_SIZE_LENGTH = Integer.BYTES;

    /**
     * Estimate the expected encoded size in bytes of this {@code Frame}.<br>
     * In particular, written data by {@link #encode(ByteBufAccessor)} should be the same as reported by this method.
     */
    int estimateEncodedSize();

    /**
     * Write the frame, including the size prefix, to the given buffer
     * @param out The output buffer
     */
    void encode(ByteBufAccessor out);

    /**
     * The correlation id.
     * @return The correlation id.
     */
    int correlationId();

    /** Api key id of this frame. */
    short apiKeyId();

    /** Api version of this frame. */
    short apiVersion();

    /** true if this frame is decoded, false otherwise */
    boolean isDecoded();
}
