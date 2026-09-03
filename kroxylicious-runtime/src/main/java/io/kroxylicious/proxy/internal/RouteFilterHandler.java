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
import io.kroxylicious.proxy.frame.PathElement;
import io.kroxylicious.proxy.internal.filter.FilterAndInvoker;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * A route-scoped {@link FilterHandler} that only applies its filter when the frame's
 * {@link Frame#routing()} lies on this handler's own route (i.e. this route's position is an
 * ancestor of, or the same as, the frame's route position). Frames on other routes (or with no
 * routing value) pass through unchanged.
 */
class RouteFilterHandler extends FilterHandler {

    private final PathElement.Route routePath;
    private final int ordinal;

    RouteFilterHandler(FilterAndInvoker filterAndInvoker,
                       long timeoutMs,
                       @Nullable String sniHostname,
                       Channel inboundChannel,
                       ClientConnectionStateMachine clientConnectionStateMachine,
                       PathElement.Route routePath,
                       int ordinal) {
        super(filterAndInvoker, timeoutMs, sniHostname, inboundChannel, clientConnectionStateMachine);
        this.routePath = Objects.requireNonNull(routePath);
        this.ordinal = ordinal;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (matchesRoute(msg)) {
            super.channelRead(ctx, msg);
        }
        else {
            ctx.fireChannelRead(msg);
        }
    }

    @Override
    PathElement.Route ownRoutePath() {
        return routePath;
    }

    @Override
    int ownOrdinal() {
        return ordinal;
    }

    // FutureReturnValueIgnored: `promise` is supplied by the caller and is notified with
    // the outcome of the write, so the returned future carries no additional information.
    @SuppressWarnings("FutureReturnValueIgnored")
    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (matchesRoute(msg)) {
            super.write(ctx, msg, promise);
        }
        else {
            ctx.write(msg, promise);
        }
    }

    private boolean matchesRoute(Object msg) {
        return msg instanceof Frame f && f.routing() != null && routePath.isAncestorOfOrSameAs(f.routing());
    }

    @Override
    String filterDescriptor() {
        return super.filterDescriptor() + "[route=" + routePath.describe() + "]";
    }
}
