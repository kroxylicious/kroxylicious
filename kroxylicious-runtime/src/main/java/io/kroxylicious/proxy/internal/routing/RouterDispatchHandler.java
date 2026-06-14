/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.routing;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.AttributeKey;

import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.frame.DecodedResponseFrame;
import io.kroxylicious.proxy.internal.ClientConnectionStateMachine;
import io.kroxylicious.proxy.router.Router;
import io.kroxylicious.proxy.router.RouterContext;

/**
 * Sits at the end of the VC-level filter chain (replacing
 * {@link io.kroxylicious.proxy.internal.FilterChainCompletionHandler}) when a
 * virtual cluster uses a router. Unwraps incoming
 * {@link DecodedRequestFrame}s and invokes {@link Router#onRequest(ApiKeys, short, RequestHeaderData, ApiMessage, RouterContext)}.
 */
public class RouterDispatchHandler extends ChannelInboundHandlerAdapter implements RoutingResponseCallback {

    private static final Logger LOGGER = LoggerFactory.getLogger(RouterDispatchHandler.class);
    private static final AttributeKey<Map<Integer, CompletableFuture<ApiMessage>>> PENDING_RESPONSES = AttributeKey.valueOf(RouterDispatchHandler.class,
            "pendingResponses");

    private final Router router;
    private final Map<String, RouteDescriptor> routes;
    private final ClientConnectionStateMachine ccsm;

    public RouterDispatchHandler(Router router,
                                 Map<String, RouteDescriptor> routes,
                                 ClientConnectionStateMachine ccsm) {
        this.router = router;
        this.routes = routes;
        this.ccsm = ccsm;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (!(msg instanceof DecodedRequestFrame<?> frame)) {
            LOGGER.atWarn()
                    .addKeyValue("sessionId", ccsm.sessionId())
                    .addKeyValue("messageClass", msg.getClass().getName())
                    .log("RouterDispatchHandler received non-DecodedRequestFrame");
            ccsm.onClientFilterChainComplete(msg);
            return;
        }

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
                forwarded -> ccsm.onClientFilterChainComplete(forwarded));

        router.onRequest(
                apiKey,
                apiVersion,
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
                    handleRouterResult(ctx, correlationId, apiVersion, (RouterResultImpl) result);
                });
    }

    private void handleRouterResult(ChannelHandlerContext ctx,
                                    int correlationId,
                                    short apiVersion,
                                    RouterResultImpl result) {
        ApiMessage body = result.body();
        if (body != null) {
            ResponseHeaderData header = result.header();
            if (header == null) {
                header = new ResponseHeaderData();
            }
            header.setCorrelationId(correlationId);
            var responseFrame = new DecodedResponseFrame<>(
                    apiVersion,
                    correlationId,
                    header,
                    body);
            ctx.channel().write(responseFrame);
            ctx.channel().flush();
        }
        if (result.closeConnection()) {
            ctx.channel().close();
        }
    }

    @Override
    public void onResponse(Object msg) {
        if (msg instanceof DecodedResponseFrame<?> frame) {
            int correlationId = frame.correlationId();
            Map<Integer, CompletableFuture<ApiMessage>> pending = getPendingResponses(ccsm.clientChannel());
            CompletableFuture<ApiMessage> future = pending.remove(correlationId);
            if (future != null) {
                future.complete(frame.body());
            }
            else {
                LOGGER.atWarn()
                        .addKeyValue("sessionId", ccsm.sessionId())
                        .addKeyValue("correlationId", correlationId)
                        .log("Received response with no pending future");
            }
        }
    }

    static void registerPendingResponse(Channel channel,
                                        int correlationId,
                                        CompletableFuture<ApiMessage> future) {
        getPendingResponses(channel).put(correlationId, future);
    }

    private static Map<Integer, CompletableFuture<ApiMessage>> getPendingResponses(Channel channel) {
        var attr = channel.attr(PENDING_RESPONSES);
        Map<Integer, CompletableFuture<ApiMessage>> map = attr.get();
        if (map == null) {
            map = new ConcurrentHashMap<>();
            attr.set(map);
        }
        return map;
    }
}
