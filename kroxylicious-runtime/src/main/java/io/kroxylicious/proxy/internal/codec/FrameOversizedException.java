/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.codec;

/**
 * Thrown by the decoder when the size of a received frame exceeds the configured maximum frame size.
 */
public class FrameOversizedException extends RuntimeException {

    /** The maximum permitted frame size in bytes. */
    private final int maxFrameSizeBytes;
    /** The size in bytes of the received frame. */
    private final int receivedFrameSizeBytes;

    /**
     * Constructs a FrameOversizedException.
     * @param maxFrameSizeBytes The maximum permitted frame size in bytes.
     * @param receivedFrameSizeBytes The size in bytes of the received frame.
     */
    public FrameOversizedException(int maxFrameSizeBytes, int receivedFrameSizeBytes) {
        super("received frame with size in bytes: " + receivedFrameSizeBytes + " but maximum size in bytes is: " + maxFrameSizeBytes);
        this.maxFrameSizeBytes = maxFrameSizeBytes;
        this.receivedFrameSizeBytes = receivedFrameSizeBytes;
    }

    /**
     * The maximum permitted frame size in bytes.
     * @return The maximum permitted frame size in bytes.
     */
    public int getMaxFrameSizeBytes() {
        return maxFrameSizeBytes;
    }

    /**
     * The size in bytes of the received frame.
     * @return The size in bytes of the received frame.
     */
    public int getReceivedFrameSizeBytes() {
        return receivedFrameSizeBytes;
    }
}
