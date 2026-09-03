/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.routing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;

import io.kroxylicious.proxy.frame.DecodedFrame;
import io.kroxylicious.proxy.frame.Frame;
import io.kroxylicious.proxy.frame.PathElement;
import io.kroxylicious.proxy.internal.ClientConnectionStateMachine;

import static java.util.Objects.requireNonNull;

/**
 * Sits at the end of the routing section of the pipeline.
 * <p>
 * On the inbound (request) path it reads the frame's route position and forwards to the
 * {@link ClientConnectionStateMachine} via the appropriate method.
 * <p>
 * On the outbound (response) path, backend responses arrive via the normal {@code channel.write()}
 * path, already carrying the correct routing value - restored directly from
 * {@code CorrelationManager} at decode time - so no bookkeeping is needed here; this handler is a
 * pure pass-through outbound.
 */
public class RoutingTerminalHandler extends ChannelDuplexHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(RoutingTerminalHandler.class);

    private final ClientConnectionStateMachine ccsm;

    /**
     * Creates a routing terminal handler.
     *
     * @param ccsm the state machine for the client connection that routed frames are forwarded to
     */
    public RoutingTerminalHandler(ClientConnectionStateMachine ccsm) {
        this.ccsm = requireNonNull(ccsm);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (msg instanceof Frame frame) {
            PathElement routing = frame.routing();
            PathElement.RoutePosition position = routing == null ? null : routing.routePosition();
            if (!(position instanceof PathElement.Route route)) {
                ccsm.onClientFilterChainComplete(msg);
                return;
            }
            String routeName = route.name();
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
}
