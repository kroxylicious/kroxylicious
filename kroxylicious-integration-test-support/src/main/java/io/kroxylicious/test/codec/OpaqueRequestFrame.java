/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.test.codec;

import java.util.concurrent.CompletableFuture;

import org.apache.kafka.common.protocol.ApiKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;

import io.kroxylicious.test.client.SequencedResponse;

/**
 * A frame in the Kafka protocol which has not been decoded.
 * The wrapped buffer <strong>does not</strong> include the frame size prefix.
 */
public class OpaqueRequestFrame implements RequestFrame {

    private static final Logger LOGGER = LoggerFactory.getLogger(OpaqueRequestFrame.class);

    /**
     * Number of bytes required for storing the frame length.
     */
    private static final int FRAME_SIZE_LENGTH = Integer.BYTES;

    protected final int length;
    protected final int correlationId;
    /** The message buffer excluding the frame size, including the header and body. */
    protected final ByteBuf buf;
    private final boolean hasResponse;
    private final CompletableFuture<SequencedResponse> responseFuture = new CompletableFuture<>();
    private final ApiKeys apiKey;
    private final short requestApiVersion;
    private final short responseApiVersion;

    /**
     * @param buf The message buffer (excluding the frame size)
     * @param correlationId The correlation id
     * @param length The length of the frame within {@code buf}.
     * @param hasResponse do we expect a response
     * @param apiKey the API key of the frame
     * @param apiVersion the apiVersion used for the request and response
     */
    public OpaqueRequestFrame(ByteBuf buf, int correlationId, int length, boolean hasResponse, ApiKeys apiKey, short apiVersion) {
        this(buf, correlationId, length, hasResponse, apiKey, apiVersion, apiVersion);
    }

    /**
     * This is a special case for testing API Versions RPCs. In general, we should use {@link OpaqueRequestFrame#OpaqueRequestFrame(ByteBuf, int, int, boolean, ApiKeys, short)}
     * <p>
     * @param buf The message buffer (excluding the frame size)
     * @param correlationId The correlation id
     * @param length The length of the frame within {@code buf}.
     * @param hasResponse do we expect a response
     * @param apiKey the API key of the frame
     * @param requestApiVersion the api Version of the request frame being sent
     * @param responseApiVersion the expected api version of the response
     */
    public OpaqueRequestFrame(ByteBuf buf, int correlationId, int length, boolean hasResponse, ApiKeys apiKey, short requestApiVersion, short responseApiVersion) {
        this.length = length;
        this.correlationId = correlationId;
        this.buf = buf.asReadOnly();
        this.apiKey = apiKey;
        this.requestApiVersion = requestApiVersion;
        this.responseApiVersion = responseApiVersion;
        if (buf.readableBytes() != length) {
            throw new AssertionError("readable: " + buf.readableBytes() + " length: " + length);
        }
        this.hasResponse = hasResponse;
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
            LOGGER.trace("Writing {} with 4 byte length ({}) plus {} bytes from buffer {} to {}",
                    getClass().getSimpleName(), length, buf.readableBytes(), buf, out);
        }
        out.ensureWritable(estimateEncodedSize());
        out.writeInt(length);
        byte[] bytes = new byte[length];
        buf.readBytes(bytes);
        out.writeByteArray(bytes);
        buf.release();
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "(" +
                "length=" + length +
                ", buf=" + buf +
                ')';
    }

    @Override
    public CompletableFuture<SequencedResponse> getResponseFuture() {
        return responseFuture;
    }

    @Override
    public boolean hasResponse() {
        return hasResponse;
    }

    @Override
    public ApiKeys apiKey() {
        return apiKey;
    }

    @Override
    public short apiVersion() {
        return requestApiVersion;
    }

    @Override
    public short responseApiVersion() {
        return responseApiVersion;
    }
}
