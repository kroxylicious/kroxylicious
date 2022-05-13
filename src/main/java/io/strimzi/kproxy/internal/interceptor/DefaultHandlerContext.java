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
package io.strimzi.kproxy.internal.interceptor;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.strimzi.kproxy.codec.DecodedFrame;
import io.strimzi.kproxy.interceptor.HandlerContext;

public class DefaultHandlerContext implements HandlerContext {

    private final ChannelHandlerContext ctx;
    private final DecodedFrame<?, ?> decodedFrame;

    public DefaultHandlerContext(ChannelHandlerContext ctx, DecodedFrame<?, ?> decodedFrame) {
        this.ctx = ctx;
        this.decodedFrame = decodedFrame;
    }

    @Override
    public String channelDescriptor() {
        return ctx.channel().toString();
    }

    @Override
    public ByteBuf allocate(int initialCapacity) {
        final ByteBuf buffer = ctx.alloc().heapBuffer(initialCapacity);
        decodedFrame.add(buffer);
        return buffer;
    }
}
