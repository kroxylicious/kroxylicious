/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.strimzi.kproxy.codec;

import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.MessageSizeAccumulator;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.netty.buffer.ByteBuf;
import io.netty.util.AbstractReferenceCounted;
import io.netty.util.ReferenceCounted;

/**
 * A frame that has been decoded (as opposed to an {@link OpaqueFrame}).
 * @param <H>
 */
public abstract class DecodedFrame<H extends ApiMessage, B extends ApiMessage>
        extends AbstractReferenceCounted
        implements Frame {

    private static final Logger LOGGER = LogManager.getLogger(DecodedFrame.class);

    /**
     * Number of bytes required for storing the frame length.
     */
    private static final int FRAME_SIZE_LENGTH = Integer.BYTES;

    protected final H header;
    protected final B body;
    protected final short apiVersion;
    private final List<ByteBuf> buffers;
    private int headerAndBodyEncodedLength;
    private ObjectSerializationCache serializationCache;

    public DecodedFrame(short apiVersion, H header, B body) {
        this.header = header;
        this.apiVersion = apiVersion;
        this.body = body;
        this.buffers = new ArrayList<>();
        this.headerAndBodyEncodedLength = -1;
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
    public final void encode(ByteBuf out) {
        if (headerAndBodyEncodedLength < 0) {
            LOGGER.warn("Encoding estimation should happen before encoding, if possible");
        }
        final int encodedSize = estimateEncodedSize();
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("Writing {} with 4 byte length ({}) plus bytes of header {}, and body {} to {}",
                    getClass().getSimpleName(), encodedSize, header, body, out);
        }
        out.ensureWritable(encodedSize);
        final int initialIndex = out.writerIndex();
        final ByteBufAccessor writable = new ByteBufAccessor(out);
        writable.writeInt(headerAndBodyEncodedLength);
        final ObjectSerializationCache cache = serializationCache;
        header.write(writable, cache, headerVersion());
        body.write(writable, cache, apiVersion());
        assert (out.writerIndex() - initialIndex) == encodedSize;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "(" +
                ApiKeys.forId(apiVersion) + "(" + apiVersion + ")v" + apiVersion +
                ", header=" + header +
                ", body=" + body +
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
        for (ByteBuf buffer : buffers) {
            buffer.release();
        }
    }
}
