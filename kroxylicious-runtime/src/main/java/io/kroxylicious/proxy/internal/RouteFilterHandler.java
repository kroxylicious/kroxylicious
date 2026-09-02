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
 * {@link Frame#path()} lies on this handler's own route (i.e. this route's position is an
 * ancestor of, or the same as, the frame's path). Frames on other routes (or with no routing
 * context) pass through unchanged.
 */
class RouteFilterHandler extends FilterHandler {

    private final PathElement routePath;
    private final int ordinal;

    RouteFilterHandler(FilterAndInvoker filterAndInvoker,
                       long timeoutMs,
                       @Nullable String sniHostname,
                       Channel inboundChannel,
                       ClientConnectionStateMachine clientConnectionStateMachine,
                       PathElement routePath,
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
    PathElement ownRoutePath() {
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
        // Gating purely on route membership is sufficient: an out-of-band response addressed to
        // this handler's own filter always carries this route as part of its own identity (see
        // isRecipient()), so recipient frames are always also route-matching frames here - unlike
        // the old route-name-string scheme, a frame's path is set once, correctly, and never
        // needs restoring via a separate, collision-prone lookup, so there's no scenario where a
        // recipient frame's route fails to match.
        if (matchesRoute(msg)) {
            super.write(ctx, msg, promise);
        }
        else {
            ctx.write(msg, promise);
        }
    }

    private boolean matchesRoute(Object msg) {
        return msg instanceof Frame f && f.path() != null && routePath.isAncestorOfOrSameAs(f.path());
    }

    @Override
    String filterDescriptor() {
        return super.filterDescriptor() + "[route=" + routePath.describe() + "]";
    }
}
