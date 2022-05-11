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
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.strimzi.kproxy.api.filter.KrpcFilterContext;
import io.strimzi.kproxy.api.filter.KrpcResponseFilter;
import io.strimzi.kproxy.codec.DecodedResponseFrame;

/**
 * A {@code ChannelInboundHandler} (for handling responses from upstream)
 * that applies a single {@link KrpcResponseFilter}.
 */
public class SingleResponseFilterHandler extends ChannelInboundHandlerAdapter {

    private final KrpcResponseFilter filter;
    private ChannelHandlerContext channelContext;

    public SingleResponseFilterHandler(KrpcResponseFilter responseFilter) {
        this.filter = responseFilter;
    }

    public KrpcResponseFilter filter() {
        return filter;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        this.channelContext = ctx;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof DecodedResponseFrame) {
            DecodedResponseFrame<?> decodedFrame = (DecodedResponseFrame<?>) msg;

            KrpcFilterContext filterContext = new KrpcFilterContext() {
                @Override
                public String channelDescriptor() {
                    return channelContext.channel().toString();
                }

                @Override
                public ByteBuffer allocate(int initialCapacity) {
                    ByteBuf buffer = channelContext.alloc().buffer(initialCapacity);
                    ByteBuffer nioBuffer = buffer.nioBuffer();
                    decodedFrame.add(buffer);
                    return nioBuffer;
                }

                @Override
                public void forwardRequest(ApiMessage header, ApiMessage message) {

                }

                @Override
                public void forwardResponse(ApiMessage header, ApiMessage message) {

                }
            };

            if (filter.shouldDeserializeResponse(decodedFrame.apiKey(), decodedFrame.apiVersion())) {
                switch (filter.apply(decodedFrame, filterContext)) {
                    case FORWARD:
                        super.channelRead(ctx, msg);
                        break;
                    case DROP:
                        break;
                    case DISCONNECT:
                        channelContext.disconnect();
                }
            }
            else {
                super.channelRead(ctx, msg);
            }
        }
        else {
            super.channelRead(ctx, msg);
        }
    }
}
