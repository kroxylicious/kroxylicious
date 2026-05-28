/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal;

import java.util.Objects;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;

import io.kroxylicious.proxy.frame.DecodedFrame;
import io.kroxylicious.proxy.frame.OpaqueFrame;
import io.kroxylicious.proxy.frame.RoutingContext;
import io.kroxylicious.proxy.internal.filter.FilterAndInvoker;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * A route-scoped {@link FilterHandler} that only applies its filter when the
 * frame's {@link RoutingContext#route()} matches the configured route name.
 * Frames on other routes (or with no routing context) pass through unchanged.
 * <p>
 * Internal filter frames ({@link InternalRequestFrame}, {@link InternalResponseFrame})
 * always delegate to the filter regardless of route — they are part of the
 * filter's own async request mechanism.
 */
class RouteFilterHandler extends FilterHandler {

    private final String routeName;

    RouteFilterHandler(FilterAndInvoker filterAndInvoker,
                       long timeoutMs,
                       @Nullable String sniHostname,
                       Channel inboundChannel,
                       ClientConnectionStateMachine clientConnectionStateMachine,
                       String routeName) {
        super(filterAndInvoker, timeoutMs, sniHostname, inboundChannel, clientConnectionStateMachine);
        this.routeName = Objects.requireNonNull(routeName);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof InternalRequestFrame<?>) {
            super.channelRead(ctx, msg);
        }
        else if (matchesRoute(msg)) {
            super.channelRead(ctx, msg);
        }
        else {
            ctx.fireChannelRead(msg);
        }
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (msg instanceof InternalResponseFrame<?>) {
            super.write(ctx, msg, promise);
        }
        else if (matchesRoute(msg)) {
            super.write(ctx, msg, promise);
        }
        else {
            ctx.write(msg, promise);
        }
    }

    private boolean matchesRoute(Object msg) {
        RoutingContext rc = null;
        if (msg instanceof DecodedFrame<?, ?> df) {
            rc = df.routingContext();
        }
        else if (msg instanceof OpaqueFrame of) {
            rc = of.routingContext();
        }
        return rc != null && routeName.equals(rc.route());
    }

    @Override
    String filterDescriptor() {
        return super.filterDescriptor() + "[route=" + routeName + "]";
    }
}
