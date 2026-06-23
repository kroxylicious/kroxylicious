/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.routing;

import java.util.Map;

import org.apache.kafka.common.protocol.ApiKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.frame.RequestFrame;
import io.kroxylicious.proxy.internal.ClientConnectionStateMachine;

/**
 * Sits at the end of the VC-level filter chain (replacing
 * {@link io.kroxylicious.proxy.internal.FilterChainCompletionHandler}) when a
 * virtual cluster uses a router. Forwards statically-routed requests
 * directly to the {@link ClientConnectionStateMachine}; rejects any
 * request whose API key is not covered by the static routes.
 */
public class RouterDispatchHandler extends ChannelInboundHandlerAdapter {

    private static final Logger LOGGER = LoggerFactory.getLogger(RouterDispatchHandler.class);

    private final Map<ApiKeys, String> staticRoutes;
    private final ClientConnectionStateMachine ccsm;

    public RouterDispatchHandler(Map<ApiKeys, String> staticRoutes,
                                 ClientConnectionStateMachine ccsm) {
        this.staticRoutes = staticRoutes;
        this.ccsm = ccsm;
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
            if (msg instanceof DecodedRequestFrame<?> decoded) {
                throw new IllegalStateException(
                        "Dynamic routing is not supported. API key " + decoded.apiKey() + " is not covered by staticRoutes().");
            }
            LOGGER.atWarn()
                    .addKeyValue("sessionId", ccsm.sessionId())
                    .addKeyValue("apiKey", apiKey)
                    .log("Dynamically-routed API key arrived as opaque frame, forwarding to CCSM");
            ccsm.onClientFilterChainComplete(msg);
            return;
        }
        ccsm.onClientFilterChainComplete(msg);
    }
}
