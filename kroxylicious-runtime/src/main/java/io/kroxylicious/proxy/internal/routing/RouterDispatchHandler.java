/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.routing;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Meter.MeterProvider;
import io.micrometer.core.instrument.Timer;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.AttributeKey;

import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.frame.DecodedResponseFrame;
import io.kroxylicious.proxy.frame.RequestFrame;
import io.kroxylicious.proxy.internal.ClientConnectionStateMachine;
import io.kroxylicious.proxy.internal.util.Metrics;
import io.kroxylicious.proxy.routing.Response;
import io.kroxylicious.proxy.routing.Router;

/**
 * Sits at the end of the VC-level filter chain (replacing
 * {@link io.kroxylicious.proxy.internal.FilterChainCompletionHandler}) when a
 * virtual cluster uses a router. Unwraps incoming
 * {@link DecodedRequestFrame}s and invokes {@link Router#onClientRequest}.
 */
public class RouterDispatchHandler extends ChannelInboundHandlerAdapter implements RoutingResponseCallback {

    private static final Logger LOGGER = LoggerFactory.getLogger(RouterDispatchHandler.class);
    private static final AttributeKey<Map<Integer, PendingResponse>> PENDING_RESPONSES = AttributeKey.valueOf(RouterDispatchHandler.class,
            "pendingResponses");

    private final Router router;
    private final Map<String, RouteDescriptor> routes;
    private final Map<ApiKeys, String> staticRoutes;
    private final ClientConnectionStateMachine ccsm;
    private final MeterProvider<Counter> routingRequestsCounter;
    private final MeterProvider<Counter> routingErrorsCounter;
    private final MeterProvider<Timer> routingRequestDurationTimer;
    private final AtomicInteger pendingResponseCount;

    record PendingResponse(CompletableFuture<Response> future,
                           Timer.Sample timerSample,
                           String route,
                           ApiKeys apiKey) {}

    public RouterDispatchHandler(Router router,
                                 Map<String, RouteDescriptor> routes,
                                 Map<ApiKeys, String> staticRoutes,
                                 ClientConnectionStateMachine ccsm,
                                 MeterProvider<Counter> routingRequestsCounter,
                                 MeterProvider<Counter> routingErrorsCounter,
                                 MeterProvider<Timer> routingRequestDurationTimer,
                                 AtomicInteger pendingResponseCount) {
        this.router = router;
        this.routes = routes;
        this.staticRoutes = staticRoutes;
        this.ccsm = ccsm;
        this.routingRequestsCounter = routingRequestsCounter;
        this.routingErrorsCounter = routingErrorsCounter;
        this.routingRequestDurationTimer = routingRequestDurationTimer;
        this.pendingResponseCount = pendingResponseCount;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (msg instanceof RequestFrame frame) {
            ApiKeys apiKey = ApiKeys.forId(frame.apiKeyId());
            String staticRoute = staticRoutes.get(apiKey);
            if (staticRoute != null) {
                ccsm.forwardToRoute(staticRoute, msg);
                routingRequestsCounter.withTags(
                        Metrics.ROUTE_LABEL, staticRoute,
                        Metrics.ROUTING_MODE_LABEL, "static",
                        Metrics.API_KEY_LABEL, apiKey.name()).increment();
                LOGGER.atTrace()
                        .addKeyValue("sessionId", ccsm.sessionId())
                        .addKeyValue("apiKey", apiKey)
                        .addKeyValue("route", staticRoute)
                        .addKeyValue("routingMode", "static")
                        .log("Request forwarded via static route");
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

        LOGGER.atTrace()
                .addKeyValue("sessionId", ccsm.sessionId())
                .addKeyValue("apiKey", apiKey)
                .addKeyValue("apiVersion", apiVersion)
                .addKeyValue("clientCorrelationId", correlationId)
                .addKeyValue("routingMode", "dynamic")
                .log("Dispatching request to router");

        var routingContext = new RoutingContextImpl(
                correlationId,
                apiVersion,
                ctx.channel(),
                ccsm.sessionId(),
                ccsm.authenticatedSubject(),
                routes,
                (routeName, forwarded) -> ccsm.forwardToRoute(routeName, forwarded),
                routingRequestsCounter,
                routingErrorsCounter,
                routingRequestDurationTimer,
                pendingResponseCount);

        router.onClientRequest(
                apiVersion,
                apiKey,
                frame.header(),
                frame.body(),
                routingContext).whenComplete((result, error) -> {
                    if (error != null) {
                        routingErrorsCounter.withTags(
                                Metrics.ERROR_TYPE_LABEL, "router_failed").increment();
                        LOGGER.atError()
                                .addKeyValue("sessionId", ccsm.sessionId())
                                .addKeyValue("apiKey", apiKey)
                                .addKeyValue("clientCorrelationId", correlationId)
                                .setCause(error)
                                .log("Router returned failed future");
                        ctx.channel().close();
                    }
                    else {
                        LOGGER.atTrace()
                                .addKeyValue("sessionId", ccsm.sessionId())
                                .addKeyValue("apiKey", apiKey)
                                .addKeyValue("clientCorrelationId", correlationId)
                                .log("Router completed request handling");
                    }
                });
    }

    @Override
    public boolean onResponse(Object msg) {
        if (msg instanceof DecodedResponseFrame<?> frame) {
            int correlationId = frame.correlationId();
            Map<Integer, PendingResponse> pending = getPendingResponses(ccsm.clientChannel());
            PendingResponse pendingResponse = pending.remove(correlationId);
            if (pendingResponse != null) {
                pendingResponseCount.decrementAndGet();
                pendingResponse.timerSample().stop(routingRequestDurationTimer.withTags(
                        Metrics.ROUTE_LABEL, pendingResponse.route(),
                        Metrics.API_KEY_LABEL, pendingResponse.apiKey().name()));
                Response response = new ResponseImpl(
                        (ResponseHeaderData) frame.header(),
                        frame.body());
                pendingResponse.future().complete(response);
                LOGGER.atTrace()
                        .addKeyValue("sessionId", ccsm.sessionId())
                        .addKeyValue("clientCorrelationId", correlationId)
                        .log("Routed response matched to pending request");
                return true;
            }
            else if (!pending.isEmpty()) {
                routingErrorsCounter.withTags(
                        Metrics.ERROR_TYPE_LABEL, "unmatched_response").increment();
                LOGGER.atWarn()
                        .addKeyValue("sessionId", ccsm.sessionId())
                        .addKeyValue("clientCorrelationId", correlationId)
                        .log("Received response with no pending routing future");
            }
        }
        return false;
    }

    static void registerPendingResponse(Channel channel,
                                        int correlationId,
                                        PendingResponse pendingResponse) {
        getPendingResponses(channel).put(correlationId, pendingResponse);
    }

    private static Map<Integer, PendingResponse> getPendingResponses(Channel channel) {
        var attr = channel.attr(PENDING_RESPONSES);
        Map<Integer, PendingResponse> map = attr.get();
        if (map == null) {
            map = new ConcurrentHashMap<>();
            attr.set(map);
        }
        return map;
    }
}
