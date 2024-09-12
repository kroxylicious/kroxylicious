/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.frame;

import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.MessageSizeAccumulator;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.util.AbstractReferenceCounted;
import io.netty.util.ReferenceCounted;

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

    /**
     * Number of bytes required for storing the frame length.
     */
    private static final int FRAME_SIZE_LENGTH = Integer.BYTES;

    protected final short apiVersion;
    protected final int correlationId;
    protected final H header;
    protected final B body;

    private final List<ByteBuf> buffers;
    private int headerAndBodyEncodedLength;
    private ObjectSerializationCache serializationCache;

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

    protected abstract short headerVersion();

    public H header() {
        return header;
    }

    public B body() {
        return body;
    }

    public ApiKeys apiKey() {
        return ApiKeys.forId(body.apiKey());
    }

    public short apiVersion() {
        return apiVersion;
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
            LOGGER.warn("Encoding estimation should happen before encoding, if possible");
        }
        final int encodedSize = estimateEncodedSize();
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(
                    "Writing {} with 4 byte length ({}) plus bytes of header {}, and body {} to {}",
                    getClass().getSimpleName(),
                    encodedSize,
                    header,
                    body,
                    out
            );
        }
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
        return getClass().getSimpleName()
               + "("
               +
               apiKey()
               + "("
               + apiVersion
               + ")v"
               + apiVersion
               +
               ", header="
               + header
               +
               ", body="
               + body
               +
               ')';
    }

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

    public void transferBuffersTo(DecodedFrame<?, ?> frame) {
        frame.buffers.addAll(this.buffers);
        this.buffers.clear();
    }
}
