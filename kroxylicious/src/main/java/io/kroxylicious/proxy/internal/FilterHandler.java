/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal;

import java.util.Objects;

import org.apache.kafka.common.protocol.ApiMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;

import io.kroxylicious.proxy.filter.KrpcFilter;
import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.frame.DecodedResponseFrame;
import io.kroxylicious.proxy.frame.OpaqueRequestFrame;
import io.kroxylicious.proxy.frame.OpaqueResponseFrame;
import io.kroxylicious.proxy.future.InternalFuture;
import io.kroxylicious.proxy.internal.util.Assertions;

/**
 * A {@code ChannelInboundHandler} (for handling requests from downstream)
 * that applies a single {@link KrpcFilter}.
 */
public class FilterHandler
        extends ChannelDuplexHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(FilterHandler.class);
    private final KrpcFilter filter;
    private final long timeoutMs;
    private final String sniHostname;

    public FilterHandler(KrpcFilter filter, long timeoutMs, String sniHostname) {
        this.filter = Objects.requireNonNull(filter);
        this.timeoutMs = Assertions.requireStrictlyPositive(timeoutMs, "timeout");
        this.sniHostname = sniHostname;
    }

    String filterDescriptor() {
        return filter.getClass().getSimpleName() + "@" + System.identityHashCode(filter);
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (msg instanceof DecodedRequestFrame) {
            DecodedRequestFrame<?> decodedFrame = (DecodedRequestFrame<?>) msg;
            // Guard against invoking the filter unexpectedly
            if (filter.shouldDeserializeRequest(decodedFrame.apiKey(), decodedFrame.apiVersion())) {
                var filterContext = new DefaultFilterContext(filter, ctx, decodedFrame, promise, timeoutMs, sniHostname);
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("{}: Dispatching downstream {} request to filter{}: {}",
                            ctx.channel(), decodedFrame.apiKey(), filterDescriptor(), msg);
                }
                filter.onRequest(decodedFrame.apiKey(), decodedFrame.header(), decodedFrame.body(), filterContext);
            }
            else {
                ctx.write(msg, promise);
            }
        }
        else {
            if (!(msg instanceof OpaqueRequestFrame)
                    && msg != Unpooled.EMPTY_BUFFER) {
                // Unpooled.EMPTY_BUFFER is used by KafkaProxyFrontendHandler#closeOnFlush
                // but otherwise we don't expect any other kind of message
                LOGGER.warn("Unexpected message writing to upstream: {}", msg, new IllegalStateException());
            }
            ctx.write(msg, promise);
        }
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof DecodedResponseFrame) {
            DecodedResponseFrame<?> decodedFrame = (DecodedResponseFrame<?>) msg;
            if (decodedFrame instanceof InternalResponseFrame) {
                InternalResponseFrame<?> frame = (InternalResponseFrame<?>) decodedFrame;
                if (frame.isRecipient(filter)) {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("{}: Completing {} response for request sent by this filter{}: {}",
                                ctx.channel(), decodedFrame.apiKey(), filterDescriptor(), msg);
                    }
                    InternalFuture<ApiMessage> p = frame.promise();
                    p.internalComplete(decodedFrame.body());
                }
                else {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("{}: Not completing {} response for request sent by another filter {}",
                                ctx.channel(), decodedFrame.apiKey(), frame.recipient());
                    }
                    ctx.fireChannelRead(msg);
                }
            }
            else if (filter.shouldDeserializeResponse(decodedFrame.apiKey(), decodedFrame.apiVersion())) {
                var filterContext = new DefaultFilterContext(filter, ctx, decodedFrame, null, timeoutMs, sniHostname);
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("{}: Dispatching upstream {} response to filter {}: {}",
                            ctx.channel(), decodedFrame.apiKey(), filterDescriptor(), msg);
                }
                filter.onResponse(decodedFrame.apiKey(), decodedFrame.header(), decodedFrame.body(), filterContext);
            }
            else {
                ctx.fireChannelRead(msg);
            }
        }
        else {
            if (!(msg instanceof OpaqueResponseFrame)) {
                LOGGER.warn("Unexpected message reading from upstream: {}", msg, new IllegalStateException());
            }
            ctx.fireChannelRead(msg);
        }
    }

}
