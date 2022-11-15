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

import io.kroxylicious.proxy.filter.KrpcFilter;
import io.kroxylicious.proxy.frame.DecodedFrame;
import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.frame.DecodedResponseFrame;
import io.kroxylicious.proxy.frame.OpaqueRequestFrame;
import io.kroxylicious.proxy.frame.OpaqueResponseFrame;
import io.kroxylicious.proxy.future.Promise;
import io.kroxylicious.proxy.internal.util.Assertions;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;

/**
 * A {@code ChannelInboundHandler} (for handling requests from downstream)
 * that applies a single {@link KrpcFilter}.
 */
public class FilterHandler
        extends FilterInvokerHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(FilterHandler.class);

    private final long timeoutMs;

    public FilterHandler(KrpcFilter filter, long timeoutMs) {
        super(Objects.requireNonNull(filter));
        this.timeoutMs = Assertions.requireStrictlyPositive(timeoutMs, "timeout");
    }

    String filterDescriptor() {
        return filter.toString();
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (msg instanceof DecodedRequestFrame) {
            DecodedRequestFrame<?> decodedFrame = (DecodedRequestFrame<?>) msg;
            // Guard against invoking the filter unexpectedly
            if (!maybeInvoke(ctx, msg, decodedFrame, promise)) {
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

    private boolean maybeInvoke(ChannelHandlerContext ctx,
                                Object msg,
                                DecodedFrame<?, ?> decodedFrame,
                                ChannelPromise promise) {

        if (consumes(decodedFrame)) {
            var filterContext = new DefaultFilterContext(filter, ctx, decodedFrame, promise, timeoutMs);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("{}: Dispatching {} {} to filter {}: {}",
                        ctx.channel(), decodedFrame.type().isRequest() ? "downstream" : "upstream",
                        decodedFrame.type(), filterDescriptor(), msg);
            }
            invoke(decodedFrame, filterContext);
            return true;
        }
        else {
            return false;
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
                    Promise<ApiMessage> p = frame.promise();
                    p.tryComplete(decodedFrame.body());
                }
                else {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("{}: Not completing {} response for request sent by another filter {}",
                                ctx.channel(), decodedFrame.apiKey(), frame.recipient());
                    }
                    ctx.fireChannelRead(msg);
                }
            }
            else {
                if (!maybeInvoke(ctx, msg, decodedFrame, null)) {
                    ctx.fireChannelRead(msg);
                }
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
