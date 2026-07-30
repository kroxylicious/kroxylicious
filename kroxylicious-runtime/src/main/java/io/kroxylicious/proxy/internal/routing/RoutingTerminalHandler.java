/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.routing;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;

import io.kroxylicious.proxy.frame.DecodedFrame;
import io.kroxylicious.proxy.frame.Frame;
import io.kroxylicious.proxy.frame.RequestFrame;
import io.kroxylicious.proxy.internal.ClientConnectionStateMachine;
import io.kroxylicious.proxy.internal.CorrelationIdSpace;

import static java.util.Objects.requireNonNull;

/**
 * Sits at the end of the routing section of the pipeline.
 * <p>
 * On the inbound (request) path it reads the frame's route name
 * and forwards to the {@link ClientConnectionStateMachine} via the
 * appropriate method, recording the correlation ID → route mapping for
 * requests that expect a response.
 * <p>
 * On the outbound (response) path, backend responses arrive via the normal
 * {@code channel.write()} path. This handler tags each response with the
 * route name looked up from the correlation ID map, so that upstream route
 * filter handlers and the dispatch handler can identify which route the
 * response belongs to.
 */
public class RoutingTerminalHandler extends ChannelDuplexHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(RoutingTerminalHandler.class);

    private final ClientConnectionStateMachine ccsm;
    private final Map<Integer, String> correlationIdToRoute = new HashMap<>();

    public RoutingTerminalHandler(ClientConnectionStateMachine ccsm) {
        this.ccsm = requireNonNull(ccsm);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (msg instanceof Frame frame) {
            String routeName = frame.routeName();
            if (routeName == null) {
                ccsm.onClientFilterChainComplete(msg);
                return;
            }
            boolean hasResponse = !(msg instanceof RequestFrame rf) || rf.hasResponse();
            boolean isRouterInternal = CorrelationIdSpace.isRoutingCorrelationId(frame.correlationId());
            if (hasResponse && !isRouterInternal) {
                correlationIdToRoute.put(frame.correlationId(), routeName);
            }
            int targetNodeId = (frame instanceof DecodedFrame<?, ?> df) ? df.targetVirtualNodeId() : Frame.NO_TARGET_VIRTUAL_NODE_ID;
            if (targetNodeId >= 0) {
                ccsm.forwardToNode(targetNodeId, routeName, msg);
                LOGGER.atTrace()
                        .addKeyValue("sessionId", ccsm.sessionId())
                        .addKeyValue("route", routeName)
                        .addKeyValue("virtualNodeId", targetNodeId)
                        .log("Terminal forwarded to target node");
            }
            else {
                ccsm.forwardToRoute(routeName, msg);
                LOGGER.atTrace()
                        .addKeyValue("sessionId", ccsm.sessionId())
                        .addKeyValue("route", routeName)
                        .log("Terminal forwarded to route bootstrap");
            }
        }
        else {
            ccsm.onClientFilterChainComplete(msg);
        }
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (msg instanceof Frame frame) {
            String route = correlationIdToRoute.remove(frame.correlationId());
            if (route != null) {
                frame.setRouteName(route);
            }
        }
        ctx.write(msg, promise);
    }
}
