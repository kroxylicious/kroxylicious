/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.routing;

import java.util.Map;

import org.apache.kafka.common.protocol.ApiKeys;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import io.kroxylicious.proxy.frame.RequestFrame;
import io.kroxylicious.proxy.internal.ClientConnectionStateMachine;
import io.kroxylicious.proxy.router.Router;

/**
 * Sits at the end of the VC-level filter chain (instead of
 * {@link io.kroxylicious.proxy.internal.FilterChainCompletionHandler}) when a
 * virtual cluster uses a router. Forwards statically-routed requests
 * directly to the {@link ClientConnectionStateMachine}; rejects any
 * request whose API key is not covered by the static routes.
 */
public class RouterDispatchHandler extends ChannelInboundHandlerAdapter {

    private final Router router;
    private final Map<ApiKeys, String> staticRoutes;
    private final ClientConnectionStateMachine ccsm;

    public RouterDispatchHandler(Router router,
                                 Map<ApiKeys, String> staticRoutes,
                                 ClientConnectionStateMachine ccsm) {
        this.router = router;
        this.staticRoutes = staticRoutes;
        this.ccsm = ccsm;
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) {
        router.close();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (msg instanceof RequestFrame frame) {
            ApiKeys apiKey = ApiKeys.forId(frame.apiKeyId());
            String staticRoute = staticRoutes.get(apiKey);
            if (staticRoute != null) {
                ccsm.forwardToRoute(staticRoute, msg);
                return;
            }
            throw new IllegalStateException(
                    "Dynamic routing is not supported. API key " + apiKey + " is not covered by staticRoutes().");
        }
        throw new IllegalStateException(
                "Unexpected non-frame message in routing pipeline: " + msg.getClass().getName());
    }
}
