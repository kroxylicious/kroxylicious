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
package io.kroxylicious.proxy.internal;

import org.apache.kafka.common.protocol.ApiMessage;

import io.kroxylicious.proxy.filter.KrpcFilterContext;
import io.kroxylicious.proxy.frame.DecodedFrame;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;

/**
 * Implementation of {@link KrpcFilterContext}.
 */
class DefaultFilterContext implements KrpcFilterContext, AutoCloseable {

    private final DecodedFrame<?, ?> decodedFrame;
    private final ChannelHandlerContext channelContext;
    private final ChannelPromise promise;
    private boolean closed;

    DefaultFilterContext(ChannelHandlerContext channelContext,
                         DecodedFrame<?, ?> decodedFrame,
                         ChannelPromise promise) {
        this.channelContext = channelContext;
        this.decodedFrame = decodedFrame;
        this.promise = promise;
        this.closed = false;
    }

    /**
     * Get a description of the channel, typically used for logging.
     * @return a description of the channel.
     */
    @Override
    public String channelDescriptor() {
        checkNotClosed();
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
    public ByteBuf allocate(int initialCapacity) {
        checkNotClosed();
        final ByteBuf buffer = channelContext.alloc().heapBuffer(initialCapacity);
        decodedFrame.add(buffer);
        return buffer;
    }

    /**
     * Forward a request to the next filter in the chain
     * (or to the upstream broker).
     * @param message The message
     */
    @Override
    public void forwardRequest(ApiMessage message) {
        checkNotClosed();
        if (decodedFrame.body() != message) {
            throw new IllegalStateException();
        }
        // check it's a request
        String name = message.getClass().getName();
        if (!name.endsWith("RequestData")) {
            throw new AssertionError("Attempt to use forwardRequest with a non-request: " + name);
        }

        // TODO check we've not forwarded it already
        channelContext.write(decodedFrame, promise);
    }

    /**
     * Forward a request to the next filter in the chain
     * (or to the downstream client).
     * @param message The message
     */
    @Override
    public void forwardResponse(ApiMessage message) {
        checkNotClosed();
        // check it's a response
        String name = message.getClass().getName();
        if (!name.endsWith("ResponseData")) {
            throw new AssertionError("Attempt to use forwardResponse with a non-response: " + name);
        }
        // TODO check we've not forwarded it already

        channelContext.fireChannelRead(decodedFrame);
    }

    private void checkNotClosed() {
        if (closed) {
            throw new IllegalStateException("Context is closed");
        }
    }

    @Override
    public void close() {
        this.closed = true;
    }
}
