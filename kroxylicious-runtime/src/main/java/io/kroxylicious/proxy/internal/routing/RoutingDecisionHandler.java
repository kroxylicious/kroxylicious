/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.routing;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntUnaryOperator;
import java.util.stream.Collectors;

import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Meter.MeterProvider;
import io.micrometer.core.instrument.Timer;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;

import io.kroxylicious.proxy.bootstrap.RouterChainFactory;
import io.kroxylicious.proxy.frame.DecodedFrame;
import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.frame.DecodedResponseFrame;
import io.kroxylicious.proxy.frame.OpaqueFrame;
import io.kroxylicious.proxy.frame.RequestFrame;
import io.kroxylicious.proxy.frame.RoutingContext;
import io.kroxylicious.proxy.internal.ClientConnectionStateMachine;
import io.kroxylicious.proxy.internal.util.Metrics;
import io.kroxylicious.proxy.router.Response;
import io.kroxylicious.proxy.router.Router;
import io.kroxylicious.proxy.router.RouterResult;
import io.kroxylicious.proxy.service.HostPort;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * A {@link ChannelDuplexHandler} that executes a {@link Router} as part
 * of the pipeline-based routing architecture. One handler per Router,
 * installed in topological order.
 * <p>
 * <b>Inbound (channelRead):</b> activates only for frames whose
 * {@link RoutingContext#route()} matches the configured activation route.
 * Static routes are re-tagged and forwarded downstream. Dynamic routes
 * invoke {@link Router#onRequest} and fire resulting frames downstream
 * through route filters.
 * <p>
 * <b>Outbound (write):</b> intercepts responses with negative routing
 * correlation IDs that match pending dynamic requests. Completes the
 * future, and when the Router's callback produces a composed response,
 * writes it upstream with the activation route restored.
 */
public class RoutingDecisionHandler extends ChannelDuplexHandler implements PendingResponseRegistry {

    private static final Logger LOGGER = LoggerFactory.getLogger(RoutingDecisionHandler.class);

    private final String activationRoute;
    private final Router router;
    private final Map<String, RouteDescriptor> routes;
    private final Map<ApiKeys, String> staticRoutes;
    private final ClientConnectionStateMachine ccsm;
    private final NodeIdMapping nodeIdMapping;
    private final Map<Integer, HostPort> routerNodeAddresses = new HashMap<>();
    private final Map<String, Integer> bootstrapVirtualNodeIds;
    private final MeterProvider<Counter> routingRequestsCounter;
    private final MeterProvider<Counter> routingErrorsCounter;
    private final MeterProvider<Timer> routingRequestDurationTimer;
    private final AtomicInteger pendingResponseCount;
    @Nullable
    private final RouterChainFactory routerChainFactory;
    @Nullable
    private final Map<String, Map<String, RouteDescriptor>> allRouteDescriptors;
    @Nullable
    private final String virtualClusterName;

    private final Map<String, Router> nestedRouters = new HashMap<>();
    private final Map<Integer, PendingResponse> pendingResponses = new HashMap<>();
    private int nextRoutingCorrelationId = Integer.MIN_VALUE / 2;

    public RoutingDecisionHandler(String activationRoute,
                                  Router router,
                                  Map<String, RouteDescriptor> routes,
                                  Map<ApiKeys, String> staticRoutes,
                                  ClientConnectionStateMachine ccsm,
                                  NodeIdMapping nodeIdMapping,
                                  MeterProvider<Counter> routingRequestsCounter,
                                  MeterProvider<Counter> routingErrorsCounter,
                                  MeterProvider<Timer> routingRequestDurationTimer,
                                  AtomicInteger pendingResponseCount,
                                  @Nullable RouterChainFactory routerChainFactory,
                                  @Nullable Map<String, Map<String, RouteDescriptor>> allRouteDescriptors,
                                  @Nullable String virtualClusterName) {
        this.activationRoute = activationRoute;
        this.router = router;
        this.routes = routes;
        this.staticRoutes = staticRoutes;
        this.ccsm = ccsm;
        this.nodeIdMapping = nodeIdMapping;
        this.routingRequestsCounter = routingRequestsCounter;
        this.routingErrorsCounter = routingErrorsCounter;
        this.routingRequestDurationTimer = routingRequestDurationTimer;
        this.pendingResponseCount = pendingResponseCount;
        this.routerChainFactory = routerChainFactory;
        this.allRouteDescriptors = allRouteDescriptors;
        this.virtualClusterName = virtualClusterName;
        this.bootstrapVirtualNodeIds = RouterContextImpl.computeBootstrapNodeIds(
                routes, nodeIdMapping, routerNodeAddresses, IntUnaryOperator.identity());
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) {
        var toClose = new ArrayList<>(nestedRouters.values());
        nestedRouters.clear();
        for (var nested : toClose) {
            try {
                nested.close();
            }
            catch (RuntimeException e) {
                LOGGER.atWarn()
                        .setCause(LOGGER.isDebugEnabled() ? e : null)
                        .addKeyValue("error", e.getMessage())
                        .log(LOGGER.isDebugEnabled()
                                ? "Failed to close nested router"
                                : "Failed to close nested router, increase log level to DEBUG for stacktrace");
            }
        }
        router.close();
    }

    public Optional<HostPort> resolveRouterNodeAddress(int virtualNodeId) {
        return Optional.ofNullable(routerNodeAddresses.get(virtualNodeId));
    }

    // --- Inbound (request) path ---

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (!(msg instanceof RequestFrame frame)) {
            ctx.fireChannelRead(msg);
            return;
        }
        RoutingContext rc = routingContextOf(msg);
        if (rc == null || !activationRoute.equals(rc.route())) {
            ctx.fireChannelRead(msg);
            return;
        }

        ApiKeys apiKey = ApiKeys.forId(frame.apiKeyId());
        String staticRoute = staticRoutes.get(apiKey);
        if (staticRoute != null) {
            setRoutingContext(msg, new RoutingContext.RouteBootstrap(staticRoute));
            routingRequestsCounter.withTags(
                    Metrics.ROUTE_LABEL, staticRoute,
                    Metrics.ROUTING_MODE_LABEL, "static",
                    Metrics.API_KEY_LABEL, apiKey.name()).increment();
            LOGGER.atTrace()
                    .addKeyValue("sessionId", ccsm.sessionId())
                    .addKeyValue("apiKey", apiKey)
                    .addKeyValue("route", staticRoute)
                    .addKeyValue("routingMode", "static")
                    .log("Static route selected");
            ctx.fireChannelRead(msg);
            return;
        }
        if (msg instanceof DecodedRequestFrame<?> decoded) {
            dispatchDynamically(ctx, decoded);
            return;
        }
        LOGGER.atWarn()
                .addKeyValue("sessionId", ccsm.sessionId())
                .addKeyValue("apiKey", apiKey)
                .log("Dynamically-routed API key arrived as opaque frame, passing through");
        ctx.fireChannelRead(msg);
    }

    private void dispatchDynamically(ChannelHandlerContext ctx, DecodedRequestFrame<?> frame) {
        ApiKeys apiKey = frame.apiKey();
        short apiVersion = frame.apiVersion();
        int clientCorrelationId = frame.correlationId();

        LOGGER.atTrace()
                .addKeyValue("sessionId", ccsm.sessionId())
                .addKeyValue("apiKey", apiKey)
                .addKeyValue("apiVersion", apiVersion)
                .addKeyValue("clientCorrelationId", clientCorrelationId)
                .addKeyValue("routingMode", "dynamic")
                .log("Dispatching request to router");

        // Forwarders that fire through the pipeline instead of directly to CCSM
        RouterContextImpl.RouteForwarder routeForwarder = (routeName, forwarded) -> {
            setRoutingContext(forwarded, new RoutingContext.RouteBootstrap(routeName));
            ctx.fireChannelRead(forwarded);
        };
        RouterContextImpl.NodeForwarder nodeForwarder = (virtualNodeId, routeName, forwarded) -> {
            setRoutingContext(forwarded, new RoutingContext.RouteTargetNode(routeName, virtualNodeId));
            ctx.fireChannelRead(forwarded);
        };

        var routingContext = new RouterContextImpl(
                frame,
                ctx.channel(),
                ccsm.sessionId(),
                ccsm.authenticatedSubject(),
                routes,
                routeForwarder,
                nodeForwarder,
                nodeIdMapping,
                bootstrapVirtualNodeIds,
                () -> nextRoutingCorrelationId++,
                routingRequestsCounter,
                routingErrorsCounter,
                routingRequestDurationTimer,
                pendingResponseCount,
                this,
                // ResponseSequencer not used - response ordering handled by ResponseOrderer
                new ResponseSequencer(ctx.channel()),
                routerNodeAddresses,
                IntUnaryOperator.identity(),
                hasNestedRouters() ? this::getOrCreateNestedRouterState : null);

        router.onRequest(
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
                                .addKeyValue("clientCorrelationId", clientCorrelationId)
                                .setCause(error)
                                .log("Router returned failed future");
                        ctx.channel().close();
                    }
                    else {
                        handleRouterResult(ctx, frame, clientCorrelationId, result, apiKey);
                    }
                    ccsm.onRoutedRequestComplete();
                });
    }

    private void handleRouterResult(ChannelHandlerContext ctx,
                                    DecodedRequestFrame<?> clientFrame,
                                    int clientCorrelationId,
                                    RouterResult result,
                                    ApiKeys apiKey) {
        if (result instanceof RouterResult.Completed completed) {
            Response response = completed.response();
            response.header().setCorrelationId(clientCorrelationId);
            var responseFrame = clientFrame.responseFrame(response.header(), response.body());
            responseFrame.setRoutingContext(new RoutingContext.RouteBootstrap(activationRoute));
            ctx.write(responseFrame, ctx.voidPromise());
            ctx.flush();
            LOGGER.atTrace()
                    .addKeyValue("sessionId", ccsm.sessionId())
                    .addKeyValue("apiKey", apiKey)
                    .addKeyValue("clientCorrelationId", clientCorrelationId)
                    .log("Router completed with response");
        }
        else if (result instanceof RouterResult.CompletedNoResponse) {
            LOGGER.atTrace()
                    .addKeyValue("sessionId", ccsm.sessionId())
                    .addKeyValue("apiKey", apiKey)
                    .addKeyValue("clientCorrelationId", clientCorrelationId)
                    .log("Router completed with no response (fire-and-forget)");
        }
        else if (result instanceof RouterResult.Disconnect) {
            LOGGER.atDebug()
                    .addKeyValue("sessionId", ccsm.sessionId())
                    .addKeyValue("apiKey", apiKey)
                    .addKeyValue("clientCorrelationId", clientCorrelationId)
                    .log("Router requested disconnect");
            ctx.channel().close();
        }
    }

    // --- Outbound (response) path ---

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (msg instanceof DecodedResponseFrame<?> frame) {
            int correlationId = frame.correlationId();
            if (correlationId < 0) {
                PendingResponse pending = pendingResponses.remove(correlationId);
                if (pending != null) {
                    pendingResponseCount.decrementAndGet();
                    pending.timerSample().stop(routingRequestDurationTimer.withTags(
                            Metrics.ROUTE_LABEL, pending.route(),
                            Metrics.API_KEY_LABEL, pending.apiKey().name()));
                    pending.metadataAddressCacher().cacheIfMetadata(frame.body());
                    NodeIdResponseTranslator.translate(
                            frame.body(), frame.apiVersion(),
                            pending.nodeIdMapping(), pending.route());
                    Response response = new ResponseImpl(
                            (ResponseHeaderData) frame.header(),
                            frame.body());
                    pending.future().complete(response);
                    LOGGER.atTrace()
                            .addKeyValue("sessionId", ccsm.sessionId())
                            .addKeyValue("clientCorrelationId", correlationId)
                            .log("Routed response intercepted and future completed");
                    promise.setSuccess();
                    return;
                }
            }
        }
        // Non-intercepted response: re-tag to activation route and forward upstream
        setRoutingContext(msg, new RoutingContext.RouteBootstrap(activationRoute));
        ctx.write(msg, promise);
    }

    // --- PendingResponseRegistry ---

    @Override
    public void register(int correlationId, PendingResponse pendingResponse) {
        pendingResponses.put(correlationId, pendingResponse);
    }

    @Override
    public void deregister(int correlationId) {
        pendingResponses.remove(correlationId);
    }

    // --- Nested routers ---

    private boolean hasNestedRouters() {
        return routerChainFactory != null
                && allRouteDescriptors != null
                && routes.values().stream().anyMatch(RouteDescriptor::targetsRouter);
    }

    private RouterContextImpl.NestedRouterState getOrCreateNestedRouterState(
                                                                             String routerName,
                                                                             String outerRouteName) {
        String cacheKey = outerRouteName + ":" + routerName;
        Router nested = nestedRouters.computeIfAbsent(cacheKey, k -> routerChainFactory.createRouter(routerName, virtualClusterName));

        Map<String, RouteDescriptor> nestedRoutes = allRouteDescriptors.get(routerName);
        if (nestedRoutes == null) {
            throw new IllegalStateException(
                    "No route descriptors for nested router: " + routerName);
        }

        var nestedRouteIds = nestedRoutes.entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        e -> e.getValue().id()));
        NodeIdMapping nestedNodeIdMapping = nestedRouteIds.size() > 1
                ? new BijectiveNodeIdMapping(nestedRouteIds, nestedRouteIds.size())
                : new IdentityNodeIdMapping(nestedRouteIds.keySet().iterator().next());

        IntUnaryOperator nestedTranslator = nestedVirtual -> nodeIdMapping.toVirtual(outerRouteName, nestedVirtual);

        var nestedBootstrapIds = RouterContextImpl.computeBootstrapNodeIds(
                nestedRoutes, nestedNodeIdMapping, routerNodeAddresses, nestedTranslator);

        boolean hasDeepNested = nestedRoutes.values().stream()
                .anyMatch(RouteDescriptor::targetsRouter);
        RouterContextImpl.NestedRouterProvider childProvider = hasDeepNested
                ? (rName, outerRoute) -> {
                    throw new UnsupportedOperationException(
                            "Routing depth > 2 is not yet supported (router: " + rName + ")");
                }
                : null;

        return new RouterContextImpl.NestedRouterState(
                nested, nestedRoutes, nestedNodeIdMapping,
                nestedBootstrapIds, nestedTranslator, childProvider);
    }

    // --- Helpers ---

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
