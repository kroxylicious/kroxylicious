/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.frame;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * A frame in the Kafka protocol, which may or may not be fully decoded.
 */
public interface Frame {

    /**
     * Number of bytes required for storing the frame length.
     */
    int FRAME_SIZE_LENGTH = Integer.BYTES;

    /** Sentinel indicating no specific target virtual node; the frame should be forwarded to the route's default node. */
    int NO_TARGET_VIRTUAL_NODE_ID = -1;

    /**
     * Estimate the expected encoded size in bytes of this {@code Frame}.<br>
     * In particular, written data by {@link #encode(ByteBufAccessor)} should be the same as reported by this method.
     * @return The estimated encoded size in bytes.
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

    /**
     * The api key id of this frame.
     * @return The api key id of this frame.
     */
    short apiKeyId();

    /**
     * The api version of this frame.
     * @return The api version of this frame.
     */
    short apiVersion();

    /**
     * Whether this frame has been decoded.
     * @return true if this frame is decoded, false otherwise.
     */
    boolean isDecoded();

    /**
     * The position in the routing/filter tree this frame is associated with.
     * @return The path this frame is associated with, or {@code null} if not yet routed.
     */
    default @Nullable PathElement path() {
        return null;
    }

    /**
     * Sets the position in the routing/filter tree this frame is associated with.
     * @param path The path, or {@code null} if not routed.
     */
    default void setPath(@Nullable PathElement path) {
    }
}
