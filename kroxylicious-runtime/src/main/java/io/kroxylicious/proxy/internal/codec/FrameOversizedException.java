/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.codec;

public class FrameOversizedException extends RuntimeException {

    private final int maxFrameSizeBytes;
    private final int receivedFrameSizeBytes;

    public FrameOversizedException(int maxFrameSizeBytes, int receivedFrameSizeBytes) {
        super("received frame with size in bytes: " + receivedFrameSizeBytes + " but maximum size in bytes is: " + maxFrameSizeBytes);
        this.maxFrameSizeBytes = maxFrameSizeBytes;
        this.receivedFrameSizeBytes = receivedFrameSizeBytes;
    }

    public int getMaxFrameSizeBytes() {
        return maxFrameSizeBytes;
    }

    public int getReceivedFrameSizeBytes() {
        return receivedFrameSizeBytes;
    }
}
