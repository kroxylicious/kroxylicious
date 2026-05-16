/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.routing;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.AttributeKey;

import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.frame.DecodedResponseFrame;
import io.kroxylicious.proxy.frame.RequestFrame;
import io.kroxylicious.proxy.internal.ClientConnectionStateMachine;
import io.kroxylicious.proxy.router.Response;
import io.kroxylicious.proxy.router.Router;
import io.kroxylicious.proxy.router.RouterResult;

/**
 * Sits at the end of the VC-level filter chain (replacing
 * {@link io.kroxylicious.proxy.internal.FilterChainCompletionHandler}) when a
 * virtual cluster uses a router. Unwraps incoming
 * {@link DecodedRequestFrame}s and invokes {@link Router#onClientRequest}.
 */
public class RouterDispatchHandler extends ChannelInboundHandlerAdapter implements RoutingResponseCallback {

    private static final Logger LOGGER = LoggerFactory.getLogger(RouterDispatchHandler.class);
    private static final AttributeKey<Map<Integer, CompletableFuture<Response>>> PENDING_RESPONSES = AttributeKey.valueOf(RouterDispatchHandler.class,
            "pendingResponses");

    private final Router router;
    private final Map<String, RouteDescriptor> routes;
    private final Map<ApiKeys, String> staticRoutes;
    private final ClientConnectionStateMachine ccsm;

    public RouterDispatchHandler(Router router,
                                 Map<String, RouteDescriptor> routes,
                                 Map<ApiKeys, String> staticRoutes,
                                 ClientConnectionStateMachine ccsm) {
        this.router = router;
        this.routes = routes;
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
                dispatchDynamically(ctx, decoded);
                return;
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

    private void dispatchDynamically(ChannelHandlerContext ctx, DecodedRequestFrame<?> frame) {
        ApiKeys apiKey = frame.apiKey();
        short apiVersion = frame.apiVersion();
        int correlationId = frame.correlationId();

        var routingContext = new RoutingContextImpl(
                correlationId,
                apiVersion,
                ctx.channel(),
                ccsm.sessionId(),
                ccsm.authenticatedSubject(),
                routes,
                (routeName, forwarded) -> ccsm.forwardToRoute(routeName, forwarded));

        router.onRequest(
                apiVersion,
                apiKey,
                frame.header(),
                frame.body(),
                routingContext).whenComplete((result, error) -> {
                    if (error != null) {
                        LOGGER.atError()
                                .addKeyValue("sessionId", ccsm.sessionId())
                                .addKeyValue("apiKey", apiKey)
                                .setCause(error)
                                .log("Router returned failed future");
                        ctx.channel().close();
                        return;
                    }
                    if (result instanceof RouterResult.Completed completed) {
                        var response = completed.response();
                        var responseFrame = new DecodedResponseFrame<>(
                                apiVersion,
                                correlationId,
                                response.header(),
                                response.body());
                        ctx.channel().write(responseFrame);
                        ctx.channel().flush();
                    }
                    else if (result instanceof RouterResult.Disconnect) {
                        ctx.channel().close();
                    }
                });
    }

    @Override
    public boolean onResponse(Object msg) {
        if (msg instanceof DecodedResponseFrame<?> frame) {
            int correlationId = frame.correlationId();
            Map<Integer, CompletableFuture<Response>> pending = getPendingResponses(ccsm.clientChannel());
            CompletableFuture<Response> future = pending.remove(correlationId);
            if (future != null) {
                Response response = new ResponseImpl(
                        (ResponseHeaderData) frame.header(),
                        frame.body());
                future.complete(response);
                return true;
            }
        }
        return false;
    }

    static void registerPendingResponse(Channel channel,
                                        int correlationId,
                                        CompletableFuture<Response> future) {
        getPendingResponses(channel).put(correlationId, future);
    }

    private static Map<Integer, CompletableFuture<Response>> getPendingResponses(Channel channel) {
        var attr = channel.attr(PENDING_RESPONSES);
        Map<Integer, CompletableFuture<Response>> map = attr.get();
        if (map == null) {
            map = new ConcurrentHashMap<>();
            attr.set(map);
        }
        return map;
    }
}
