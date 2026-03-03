/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.frame;

import io.netty.buffer.ByteBuf;

/**
 * Used to represent Kafka requests that the proxy does not need to decode.
 */
public class OpaqueRequestFrame extends OpaqueFrame implements RequestFrame {

    private final boolean decodeResponse;
    private final boolean hasResponse;

    /**
     * Creates an opaque request.
     *
     * @param buf The message buffer (excluding the frame size)
     * @param apiKeyId api key
     * @param apiVersion api version
     * @param correlationId correlation id
     * @param decodeResponse whether the response will be decoded
     * @param length length of the request
     * @param hasResponse true if the request expects a response
     */
    public OpaqueRequestFrame(ByteBuf buf,
                              short apiKeyId,
                              short apiVersion,
                              int correlationId,
                              boolean decodeResponse,
                              int length,
                              boolean hasResponse) {
        super(apiKeyId, apiVersion, buf, correlationId, length);
        this.decodeResponse = decodeResponse;
        this.hasResponse = hasResponse;
    }

    @Override
    public boolean decodeResponse() {
        return decodeResponse;
    }

    @Override
    public boolean hasResponse() {
        return hasResponse;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "(" +
                "length=" + length +
                ", buf=" + buf +
                ')';
    }
}
