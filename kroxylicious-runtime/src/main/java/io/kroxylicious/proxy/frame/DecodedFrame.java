/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.frame;

import java.util.ArrayList;
import java.util.List;

import io.kroxylicious.kafka.common.protocol.ApiKeys;
import io.kroxylicious.kafka.common.protocol.ApiMessage;
import io.kroxylicious.kafka.common.protocol.MessageSizeAccumulator;
import io.kroxylicious.kafka.common.protocol.ObjectSerializationCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.util.AbstractReferenceCounted;
import io.netty.util.ReferenceCounted;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * A frame that has been decoded (as opposed to an {@link OpaqueFrame}).
 *
 * @param <H> The header type
 * @param <B> The body type
 *
 */
public abstract class DecodedFrame<H extends ApiMessage, B extends ApiMessage>
        extends AbstractReferenceCounted
        implements Frame {

    private static final Logger LOGGER = LoggerFactory.getLogger(DecodedFrame.class);

    /** The api version of the message. */
    protected final short apiVersion;
    /** The correlation id of the message. */
    protected final int correlationId;
    /** The header of the message. */
    protected final H header;
    /** The body of the message. */
    protected final B body;

    private final List<ByteBuf> buffers;
    private int headerAndBodyEncodedLength;
    private @Nullable ObjectSerializationCache serializationCache;
    private @Nullable String routeName;
    private int targetVirtualNodeId = NO_TARGET_VIRTUAL_NODE_ID;

    DecodedFrame(short apiVersion, int correlationId, H header, B body) {
        this.apiVersion = apiVersion;
        this.correlationId = correlationId;
        this.header = header;
        this.body = body;
        this.buffers = new ArrayList<>();
        this.headerAndBodyEncodedLength = -1;
    }

    @Override
    public int correlationId() {
        return correlationId;
    }

    @Override
    public short apiKeyId() {
        return body.apiKey();
    }

    /**
     * The {@link ApiKeys API key} of this frame.
     * @return The API key of this frame.
     */
    public ApiKeys apiKey() {
        return ApiKeys.forId(apiKeyId());
    }

    @Override
    public short apiVersion() {
        return apiVersion;
    }

    @Override
    public boolean isDecoded() {
        return true;
    }

    /**
     * The version of the header used by this frame.
     * @return The header version.
     */
    protected abstract short headerVersion();

    /**
     * The header of this frame.
     * @return The header of this frame.
     */
    public H header() {
        return header;
    }

    /**
     * The body of this frame.
     * @return The body of this frame.
     */
    public B body() {
        return body;
    }

    @Override
    public final int estimateEncodedSize() {
        if (headerAndBodyEncodedLength != -1) {
            assert serializationCache != null;
            return FRAME_SIZE_LENGTH + headerAndBodyEncodedLength;
        }
        var headerVersion = headerVersion();
        MessageSizeAccumulator sizer = new MessageSizeAccumulator();
        ObjectSerializationCache cache = new ObjectSerializationCache();
        header().addSize(sizer, cache, headerVersion);
        body().addSize(sizer, cache, apiVersion());
        headerAndBodyEncodedLength = sizer.totalSize();
        serializationCache = cache;
        return FRAME_SIZE_LENGTH + headerAndBodyEncodedLength;
    }

    @Override
    public final void encode(ByteBufAccessor out) {
        if (headerAndBodyEncodedLength < 0) {
            LOGGER.atWarn()
                    .log("Encoding estimation should happen before encoding, if possible");
        }
        final int encodedSize = estimateEncodedSize();
        LOGGER.atTrace()
                .addKeyValue("frameType", getClass().getSimpleName())
                .addKeyValue("encodedSize", encodedSize)
                .addKeyValue("header", header::toString)
                .addKeyValue("body", body::toString)
                .addKeyValue("output", out)
                .log("Writing frame");
        out.ensureWritable(encodedSize);
        final int initialIndex = out.writerIndex();
        out.writeInt(headerAndBodyEncodedLength);
        final ObjectSerializationCache cache = serializationCache;
        header.write(out, cache, headerVersion());
        body.write(out, cache, apiVersion());
        assert (out.writerIndex() - initialIndex) == encodedSize;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "(" +
                apiKey() + "(" + apiVersion + ")v" + apiVersion +
                ", header=" + header +
                ", body=" + body +
                ')';
    }

    /**
     * Associates the given buffer with this frame, so that it is released when this frame is deallocated.
     * @param buffer The buffer to release when this frame is deallocated.
     */
    public void add(ByteBuf buffer) {
        buffers.add(buffer);
    }

    @Override
    public ReferenceCounted touch(Object hint) {
        return this;
    }

    @Override
    protected void deallocate() {
        buffers.forEach(ByteBuf::release);
    }

    @Override
    public @Nullable String routeName() {
        return routeName;
    }

    @Override
    public void setRouteName(@Nullable String routeName) {
        this.routeName = routeName;
    }

    /**
     * The virtual node ID of the specific broker this frame should be forwarded to,
     * or {@link Frame#NO_TARGET_VIRTUAL_NODE_ID} (the default) if the frame should
     * be sent to any broker on the route. Set by the router dispatch handler when
     * forwarding to a specific node via {@code sendToSpecificNode}.
     * @return The target virtual node ID, or {@link Frame#NO_TARGET_VIRTUAL_NODE_ID}.
     */
    public int targetVirtualNodeId() {
        return targetVirtualNodeId;
    }

    /**
     * Sets the target virtual node ID for this frame. See {@link #targetVirtualNodeId()}.
     * @param targetVirtualNodeId The virtual node ID of the broker this frame should be forwarded to.
     */
    public void setTargetVirtualNodeId(int targetVirtualNodeId) {
        this.targetVirtualNodeId = targetVirtualNodeId;
    }

    /**
     * Transfers ownership of this frame's associated buffers to the given frame,
     * which becomes responsible for releasing them.
     * @param frame The frame taking ownership of the buffers.
     */
    public void transferBuffersTo(DecodedFrame<?, ?> frame) {
        frame.buffers.addAll(this.buffers);
        this.buffers.clear();
    }

}
