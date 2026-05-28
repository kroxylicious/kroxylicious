/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.routing;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import io.kroxylicious.proxy.frame.DecodedFrame;
import io.kroxylicious.proxy.frame.OpaqueFrame;
import io.kroxylicious.proxy.frame.RoutingContext;

/**
 * Sits at the top of the routing section of the pipeline.
 * Tags every inbound frame with {@code RouteBootstrap("default")}
 * so that downstream VC-level route filters can match on it.
 * <p>
 * On the outbound (response) path this handler is transparent —
 * responses pass through unchanged.
 */
public class PassthroughRoutingHandler extends ChannelInboundHandlerAdapter {

    static final String DEFAULT_ROUTE = "default";
    private static final RoutingContext DEFAULT_CONTEXT = new RoutingContext.RouteBootstrap(DEFAULT_ROUTE);

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof DecodedFrame<?, ?> df) {
            df.setRoutingContext(DEFAULT_CONTEXT);
        }
        else if (msg instanceof OpaqueFrame of) {
            of.setRoutingContext(DEFAULT_CONTEXT);
        }
        ctx.fireChannelRead(msg);
    }
}
