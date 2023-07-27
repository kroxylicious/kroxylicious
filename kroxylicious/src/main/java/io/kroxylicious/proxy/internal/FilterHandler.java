/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.common.protocol.ApiMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;

import io.kroxylicious.proxy.filter.FilterAndInvoker;
import io.kroxylicious.proxy.filter.FilterInvoker;
import io.kroxylicious.proxy.filter.KrpcFilter;
import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.frame.DecodedResponseFrame;
import io.kroxylicious.proxy.frame.OpaqueRequestFrame;
import io.kroxylicious.proxy.frame.OpaqueResponseFrame;
import io.kroxylicious.proxy.internal.util.Assertions;

/**
 * A {@code ChannelInboundHandler} (for handling requests from downstream)
 * that applies a single {@link KrpcFilter}.
 */
public class FilterHandler
        extends ChannelDuplexHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(FilterHandler.class);
    public static final CompletableFuture<Void> COMPLETED_FUTURE = CompletableFuture.completedFuture(null);
    private final KrpcFilter filter;
    private final long timeoutMs;
    private final String sniHostname;
    private final FilterInvoker invoker;

    private CompletableFuture<Void> writeFuture = COMPLETED_FUTURE;
    private CompletableFuture<Void> readFuture = COMPLETED_FUTURE;

    public FilterHandler(FilterAndInvoker filterAndInvoker, long timeoutMs, String sniHostname) {
        this.filter = Objects.requireNonNull(filterAndInvoker).filter();
        this.invoker = filterAndInvoker.invoker();
        this.timeoutMs = Assertions.requireStrictlyPositive(timeoutMs, "timeout");
        this.sniHostname = sniHostname;
    }

    String filterDescriptor() {
        return filter.getClass().getSimpleName() + "@" + System.identityHashCode(filter);
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (writeFuture.isDone()) {
            writeFuture = handleWrite(ctx, msg, promise);
        }
        else {
            writeFuture = writeFuture.whenComplete((unused, throwable) -> {
                handleWrite(ctx, msg, promise);
            });
        }
    }

    private CompletableFuture<Void> handleWrite(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
        if (msg instanceof DecodedRequestFrame<?> decodedFrame) {
            var filterContext = new DefaultFilter(filter, ctx, decodedFrame, promise, timeoutMs, sniHostname);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("{}: Dispatching downstream {} request to filter{}: {}",
                        ctx.channel(), decodedFrame.apiKey(), filterDescriptor(), msg);
            }
            invoker.onRequest(decodedFrame.apiKey(), decodedFrame.apiVersion(), decodedFrame.header(), decodedFrame.body(), filterContext);
            if (!filterContext.isClosedOrDeferred()) {
                LOGGER.warn("{}: {} write context for filter {} was not completed or deferred!",
                        ctx.channel(), decodedFrame.apiKey(), filterDescriptor());
                throw new IllegalStateException("FilterContext was not completed or deferred");
            }
            return filterContext.onClose();
        }
        else {
            if (!(msg instanceof OpaqueRequestFrame)
                    && msg != Unpooled.EMPTY_BUFFER) {
                // Unpooled.EMPTY_BUFFER is used by KafkaProxyFrontendHandler#closeOnFlush
                // but otherwise we don't expect any other kind of message
                LOGGER.warn("Unexpected message writing to upstream: {}", msg, new IllegalStateException());
            }
            ctx.write(msg, promise);
            return COMPLETED_FUTURE;
        }
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (readFuture.isDone()) {
            readFuture = handleRead(ctx, msg);
        }
        else {
            readFuture = readFuture.whenComplete((unused, throwable) -> {
                handleRead(ctx, msg);
            });
        }
    }

    private CompletableFuture<Void> handleRead(ChannelHandlerContext ctx, Object msg) {
        if (msg instanceof DecodedResponseFrame<?> decodedFrame) {
            if (decodedFrame instanceof InternalResponseFrame<?> frame && frame.isRecipient(filter)) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("{}: Completing {} response for request sent by this filter{}: {}",
                            ctx.channel(), decodedFrame.apiKey(), filterDescriptor(), msg);
                }
                CompletableFuture<ApiMessage> p = frame.promise();
                p.complete(decodedFrame.body());
                return COMPLETED_FUTURE;
            }
            else {
                var filterContext = new DefaultFilter(filter, ctx, decodedFrame, null, timeoutMs, sniHostname);
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("{}: Dispatching upstream {} response to filter {}: {}",
                            ctx.channel(), decodedFrame.apiKey(), filterDescriptor(), msg);
                }
                invoker.onResponse(decodedFrame.apiKey(), decodedFrame.apiVersion(), decodedFrame.header(), decodedFrame.body(), filterContext);
                if (!filterContext.isClosedOrDeferred()) {
                    LOGGER.warn("{}: {} read context for filter {} was not completed or deferred!",
                            ctx.channel(), decodedFrame.apiKey(), filterDescriptor());
                    throw new IllegalStateException("FilterContext was not completed or deferred");
                }
                return filterContext.onClose();
            }
        }
        else {
            if (!(msg instanceof OpaqueResponseFrame)) {
                LOGGER.warn("Unexpected message reading from upstream: {}", msg, new IllegalStateException());
            }
            ctx.fireChannelRead(msg);
            return COMPLETED_FUTURE;
        }
    }

}
