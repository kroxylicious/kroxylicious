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
package io.strimzi.kproxy.internal;

import java.nio.ByteBuffer;

import org.apache.kafka.common.protocol.ApiMessage;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.strimzi.kproxy.api.filter.KrpcFilterContext;
import io.strimzi.kproxy.codec.DecodedFrame;

/**
 * Implementation of {@link KrpcFilterContext}.
 */
class DefaultFilterContext implements KrpcFilterContext {

    private final DecodedFrame<?, ?> decodedFrame;
    private final ChannelHandlerContext channelContext;

    DefaultFilterContext(ChannelHandlerContext channelContext, DecodedFrame<?, ?> decodedFrame) {
        this.channelContext = channelContext;
        this.decodedFrame = decodedFrame;
    }

    /**
     * Get a description of the channel, typically used for logging.
     * @return a description of the channel.
     */
    @Override
    public String channelDescriptor() {
        return channelContext.channel().toString();
    }

    /**
     * Allocate a buffer with the given {@code initialCapacity}.
     * The returned buffer will be released automatically
     * TODO when?
     * @param initialCapacity The initial capacity of the buffer.
     * @return The allocated buffer.
     */
    @Override
    public ByteBuffer allocate(int initialCapacity) {
        ByteBuf buffer = channelContext.alloc().buffer(initialCapacity);
        ByteBuffer nioBuffer = buffer.nioBuffer();
        decodedFrame.add(buffer);
        return nioBuffer;
    }

    /**
     * Forward a request to the next filter in the chain
     * (or to the upstream broker).
     * @param header The header
     * @param message The message
     */
    @Override
    public void forwardRequest(ApiMessage header, ApiMessage message) {
        // TODO something like ctx.fireChannelRead();
        // but how do we know where to send it?
        // and what about previous filters in the chain?
        // This also assumes that the proxy is managing at least correlation ids for itself.
        // i.e. upstream correlation ids != downstream correlation ids
        throw new UnsupportedOperationException();
    }

    /**
     * Forward a request to the next filter in the chain
     * (or to the downstream client).
     * @param header The header
     * @param message The message
     */
    @Override
    public void forwardResponse(ApiMessage header, ApiMessage message) {
        // TODO
        throw new UnsupportedOperationException();
    }
}
