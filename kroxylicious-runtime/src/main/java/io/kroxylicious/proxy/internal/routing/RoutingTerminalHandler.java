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

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import io.kroxylicious.proxy.frame.DecodedFrame;
import io.kroxylicious.proxy.frame.Frame;
import io.kroxylicious.proxy.frame.OpaqueFrame;
import io.kroxylicious.proxy.frame.RequestFrame;
import io.kroxylicious.proxy.frame.RoutingContext;
import io.kroxylicious.proxy.internal.ClientConnectionStateMachine;

import static java.util.Objects.requireNonNull;

/**
 * Sits at the end of the routing section of the pipeline. On the inbound
 * (request) path it reads the frame's {@link RoutingContext} and forwards
 * to the {@link ClientConnectionStateMachine} via the appropriate method.
 * <p>
 * For responses, the CCSM delivers backend responses to
 * {@link #onBackendResponse(Object)} which tags the frame with its route
 * and writes it back into the pipeline via {@code ctx.write}, so route
 * filters and routing decision handlers can process it on the outbound path.
 */
public class RoutingTerminalHandler extends ChannelInboundHandlerAdapter {

    private static final Logger LOGGER = LoggerFactory.getLogger(RoutingTerminalHandler.class);

    private final ClientConnectionStateMachine ccsm;
    private final Map<Integer, String> correlationIdToRoute = new HashMap<>();
    private ChannelHandlerContext ctx;

    public RoutingTerminalHandler(ClientConnectionStateMachine ccsm) {
        this.ccsm = requireNonNull(ccsm);
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        this.ctx = ctx;
        super.handlerAdded(ctx);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (msg instanceof Frame frame) {
            RoutingContext rc = routingContextOf(msg);
            if (rc == null) {
                ccsm.onClientFilterChainComplete(msg);
                return;
            }
            boolean hasResponse = !(msg instanceof RequestFrame rf) || rf.hasResponse();
            if (hasResponse) {
                correlationIdToRoute.put(frame.correlationId(), rc.route());
            }
            if (rc instanceof RoutingContext.RouteTargetNode rtn) {
                ccsm.forwardToNode(rtn.virtualNodeId(), rtn.route(), msg);
                LOGGER.atTrace()
                        .addKeyValue("sessionId", ccsm.sessionId())
                        .addKeyValue("route", rtn.route())
                        .addKeyValue("virtualNodeId", rtn.virtualNodeId())
                        .log("Terminal forwarded to target node");
            }
            else {
                ccsm.forwardToRoute(rc.route(), msg);
                LOGGER.atTrace()
                        .addKeyValue("sessionId", ccsm.sessionId())
                        .addKeyValue("route", rc.route())
                        .log("Terminal forwarded to route bootstrap");
            }
        }
        else {
            ccsm.onClientFilterChainComplete(msg);
        }
    }

    /**
     * Called by the CCSM when a backend response arrives.
     * Tags the response with its route and writes it into the pipeline
     * so it flows backward through route filters and routing decision handlers.
     */
    public void onBackendResponse(Object msg) {
        if (ctx == null) {
            LOGGER.atWarn()
                    .addKeyValue("sessionId", ccsm.sessionId())
                    .log("Backend response received before handler added to pipeline");
            return;
        }
        if (msg instanceof Frame frame) {
            String route = correlationIdToRoute.remove(frame.correlationId());
            if (route != null) {
                setRoutingContext(msg, new RoutingContext.RouteDefaultNode(route));
            }
        }
        ctx.write(msg, ctx.voidPromise());
    }

    /**
     * Called by the CCSM when a server read batch is complete.
     */
    public void flushToClient() {
        if (ctx != null) {
            ctx.flush();
        }
    }

    private static RoutingContext routingContextOf(Object msg) {
        if (msg instanceof DecodedFrame<?, ?> df) {
            return df.routingContext();
        }
        else if (msg instanceof OpaqueFrame of) {
            return of.routingContext();
        }
        return null;
    }

    private static void setRoutingContext(Object msg, RoutingContext rc) {
        if (msg instanceof DecodedFrame<?, ?> df) {
            df.setRoutingContext(rc);
        }
        else if (msg instanceof OpaqueFrame of) {
            of.setRoutingContext(rc);
        }
    }
}
