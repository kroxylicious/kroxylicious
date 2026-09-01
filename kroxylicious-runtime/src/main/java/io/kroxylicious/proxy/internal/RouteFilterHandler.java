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

import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.frame.Frame;
import io.kroxylicious.proxy.internal.filter.FilterAndInvoker;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * A route-scoped {@link FilterHandler} that only applies its filter when the
 * frame's {@link Frame#routeName()} matches the configured route name.
 * Frames on other routes (or with no routing context) pass through unchanged.
 */
class RouteFilterHandler extends FilterHandler {

    private final String routeName;
    private final Filter filter;

    RouteFilterHandler(FilterAndInvoker filterAndInvoker,
                       long timeoutMs,
                       @Nullable String sniHostname,
                       Channel inboundChannel,
                       ClientConnectionStateMachine clientConnectionStateMachine,
                       String routeName) {
        super(filterAndInvoker, timeoutMs, sniHostname, inboundChannel, clientConnectionStateMachine);
        this.routeName = Objects.requireNonNull(routeName);
        this.filter = filterAndInvoker.filter();
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
    void onInternalRequest(InternalRequestFrame<?> frame) {
        frame.setRouteName(routeName);
    }

    // FutureReturnValueIgnored: `promise` is supplied by the caller and is notified with
    // the outcome of the write, so the returned future carries no additional information.
    @SuppressWarnings("FutureReturnValueIgnored")
    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        // Frames are gated to their route, as before - this is what lets a route's filters observe,
        // via onResponse, every response (including out-of-band ones) that flows through their route.
        // The one addition: an out-of-band (internal) response addressed to this handler's own filter
        // is also delivered even when its route name does not match. All internal responses share the
        // reserved out-of-band correlation id, so the route name restored on the response path is
        // unreliable when more than one route has an in-flight OOB request; but such a reply is
        // self-addressed (its recipient and promise are carried on the frame, matched via the unique
        // upstream correlation id), so recipient identity is authoritative for delivering it back.
        boolean deliver = matchesRoute(msg)
                || (msg instanceof InternalResponseFrame<?> internalResponse && internalResponse.isRecipient(filter));
        if (deliver) {
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
