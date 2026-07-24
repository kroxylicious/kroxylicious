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

import io.kroxylicious.proxy.frame.Frame;
import io.kroxylicious.proxy.internal.filter.FilterAndInvoker;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * A route-scoped {@link FilterHandler} that only applies its filter when the
 * frame's {@link Frame#routeName()} matches the configured route name.
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
        if (msg instanceof InternalRequestFrame<?> || matchesRoute(msg)) {
            super.channelRead(ctx, msg);
        }
        else {
            ctx.fireChannelRead(msg);
        }
    }

    @Override
    void onInternalRequest(InternalRequestFrame<?> frame) {
        frame.setRouteName(routeName);
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (msg instanceof InternalResponseFrame<?> || matchesRoute(msg)) {
            super.write(ctx, msg, promise);
        }
        else {
            ctx.write(msg, promise);
        }
    }

    private boolean matchesRoute(Object msg) {
        return msg instanceof Frame f && routeName.equals(f.routeName());
    }

    @Override
    String filterDescriptor() {
        return super.filterDescriptor() + "[route=" + routeName + "]";
    }
}
