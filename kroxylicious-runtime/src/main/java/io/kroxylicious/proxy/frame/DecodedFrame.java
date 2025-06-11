/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.frame;

import org.apache.kafka.common.protocol.ApiMessage;

/**
 * A frame that has been decoded (as opposed to an {@link OpaqueFrame}).
 *
 * @param <H> The header type
 * @param <B> The body type
 *
 */
public abstract sealed class DecodedFrame<H extends ApiMessage, B extends ApiMessage>
        extends AbstractApiMessageBasedFrame<H, B>
        implements NetworkOriginatedFrame
        permits DecodedRequestFrame, DecodedResponseFrame {

    private final int originalEncodedSize;

    DecodedFrame(short apiVersion, int correlationId, H header, B body, int originalEncodedSize) {
        super(apiVersion, correlationId, header, body);
        this.originalEncodedSize = originalEncodedSize;
    }

    @Override
    public int originalEncodedSize() {
        return originalEncodedSize + FRAME_SIZE_LENGTH;
    }
}
