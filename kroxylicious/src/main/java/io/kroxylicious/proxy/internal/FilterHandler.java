/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.common.message.ResponseHeaderData;
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
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.filter.ResponseFilterResult;
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
    private final KrpcFilter filter;
    private final long timeoutMs;
    private final String sniHostname;
    private final FilterInvoker invoker;

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
        if (msg instanceof DecodedRequestFrame<?> decodedFrame) {
            var filterContext = new DefaultFilterContext(filter, ctx, decodedFrame, promise, timeoutMs, sniHostname);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("{}: Dispatching downstream {} request to filter{}: {}",
                        ctx.channel(), decodedFrame.apiKey(), filterDescriptor(), msg);
            }

            var stage = invoker.onRequest(decodedFrame.apiKey(), decodedFrame.apiVersion(), decodedFrame.header(),
                    decodedFrame.body(), filterContext);
            stage.whenComplete((filterResult, t) -> {
                // should run on netty thread?

                if (t != null) {
                    filterContext.closeConnection();
                    return;
                }

                if (filterResult instanceof RequestFilterResult rfr) {
                    var header = rfr.header() == null ? decodedFrame.header() : rfr.header();
                    filterContext.forwardRequest(header, rfr.request());
                }
                else if (filterResult instanceof ResponseFilterResult rfr) {
                    // short circuit path
                    if (rfr.response() != null) {
                        var header = rfr.header() == null ? new ResponseHeaderData().setCorrelationId(decodedFrame.correlationId()) : rfr.header();
                        filterContext.forwardResponse(header, rfr.response());
                    }

                    if (rfr.closeConnection()) {
                        filterContext.closeConnection();
                    }
                }

            });

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
        if (msg instanceof DecodedResponseFrame<?> decodedFrame) {
            if (decodedFrame instanceof InternalResponseFrame<?> frame && frame.isRecipient(filter)) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("{}: Completing {} response for request sent by this filter{}: {}",
                            ctx.channel(), decodedFrame.apiKey(), filterDescriptor(), msg);
                }
                CompletableFuture<ApiMessage> p = frame.promise();
                p.complete(decodedFrame.body());
            }
            else {
                var filterContext = new DefaultFilterContext(filter, ctx, decodedFrame, null, timeoutMs, sniHostname);
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("{}: Dispatching upstream {} response to filter {}: {}",
                            ctx.channel(), decodedFrame.apiKey(), filterDescriptor(), msg);
                }
                var stage = invoker.onResponse(decodedFrame.apiKey(), decodedFrame.apiVersion(),
                        decodedFrame.header(), decodedFrame.body(), filterContext);

                stage.whenComplete((rfr, t) -> {
                    if (t != null) {
                        filterContext.closeConnection();
                        return;
                    }
                    if (rfr.response() != null) {
                        ResponseHeaderData header = rfr.header() == null ? decodedFrame.header() : rfr.header();
                        filterContext.forwardResponse(header, rfr.response());
                    }
                    if (rfr.closeConnection()) {
                        filterContext.closeConnection();
                    }

                });

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
