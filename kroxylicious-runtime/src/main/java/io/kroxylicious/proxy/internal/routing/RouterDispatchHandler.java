/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.routing;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntUnaryOperator;
import java.util.stream.Collectors;

import org.apache.kafka.common.message.MetadataResponseData;
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

import io.kroxylicious.proxy.bootstrap.FilterChainFactory;
import io.kroxylicious.proxy.bootstrap.RouterChainFactory;
import io.kroxylicious.proxy.config.PluginFactoryRegistry;
import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.frame.DecodedResponseFrame;
import io.kroxylicious.proxy.frame.RequestFrame;
import io.kroxylicious.proxy.internal.ClientConnectionStateMachine;
import io.kroxylicious.proxy.internal.filter.FilterAndInvoker;
import io.kroxylicious.proxy.internal.filter.NettyFilterContext;
import io.kroxylicious.proxy.internal.util.Metrics;
import io.kroxylicious.proxy.router.Response;
import io.kroxylicious.proxy.router.Router;
import io.kroxylicious.proxy.router.RouterResult;
import io.kroxylicious.proxy.service.HostPort;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Sits at the end of the VC-level filter chain (replacing
 * {@link io.kroxylicious.proxy.internal.FilterChainCompletionHandler}) when a
 * virtual cluster uses a router. Unwraps incoming
 * {@link DecodedRequestFrame}s and invokes {@link Router#onRequest}.
 */
public class RouterDispatchHandler extends ChannelInboundHandlerAdapter implements RoutingResponseCallback {

    private static final Logger LOGGER = LoggerFactory.getLogger(RouterDispatchHandler.class);
    private static final AttributeKey<Map<Integer, PendingResponse>> PENDING_RESPONSES = AttributeKey.valueOf(RouterDispatchHandler.class,
            "pendingResponses");

    private final Router router;
    private final Map<ApiKeys, String> staticRoutes;
    private final ClientConnectionStateMachine ccsm;
    private final RoutingInfrastructure infra;
    @Nullable
    private final RouteFilterSupport routeFilterSupport;
    @Nullable
    private final NestedRoutingSupport nestedRoutingSupport;
    private final Map<String, Router> nestedRouters = new HashMap<>();
    private final Map<RouteDescriptor, CompletionStage<RouteFilterPipeline>> routeFilterPipelines = new HashMap<>();
    private int nextRoutingCorrelationId = Integer.MIN_VALUE / 2;
    @Nullable
    private ResponseSequencer responseSequencer;

    record PendingResponse(CompletableFuture<Response> future,
                           Timer.Sample timerSample,
                           String route,
                           ApiKeys apiKey,
                           NodeIdMapping nodeIdMapping,
                           MetadataAddressCacher metadataAddressCacher,
                           @Nullable DecodedRequestFrame<?> originatingRequestFrame,
                           @Nullable RouteFilterPipeline routeFilterPipeline) {

        static PendingResponse create(
                                      CompletableFuture<Response> future,
                                      String route,
                                      ApiKeys apiKey,
                                      NodeIdMapping nodeIdMapping,
                                      MetadataAddressCacher metadataAddressCacher,
                                      @Nullable DecodedRequestFrame<?> originatingRequestFrame,
                                      @Nullable RouteFilterPipeline routeFilterPipeline) {
            return new PendingResponse(future, Timer.start(), route, apiKey,
                    nodeIdMapping, metadataAddressCacher, originatingRequestFrame, routeFilterPipeline);
        }
    }

    /**
     * Caches broker addresses from a METADATA response before node ID translation.
     * The implementation is responsible for mapping target node IDs to the
     * appropriate virtual IDs for the CCSM address resolver.
     */
    @FunctionalInterface
    interface MetadataAddressCacher {
        void cacheIfMetadata(Object responseBody);
    }

    public record RouteFilterSupport(
                                     FilterChainFactory filterChainFactory,
                                     PluginFactoryRegistry pfr,
                                     @Nullable String sniHostname,
                                     io.netty.channel.EventLoopGroup eventLoopGroup) {}

    public record NestedRoutingSupport(
                                       RouterChainFactory routerChainFactory,
                                       Map<String, Map<String, RouteDescriptor>> allRouteDescriptors,
                                       String virtualClusterName) {}

    public RouterDispatchHandler(Router router,
                                 Map<String, RouteDescriptor> routes,
                                 Map<ApiKeys, String> staticRoutes,
                                 ClientConnectionStateMachine ccsm,
                                 NodeIdMapping nodeIdMapping,
                                 MeterProvider<Counter> routingRequestsCounter,
                                 MeterProvider<Counter> routingErrorsCounter,
                                 MeterProvider<Timer> routingRequestDurationTimer,
                                 AtomicInteger pendingResponseCount,
                                 @Nullable RouteFilterSupport routeFilterSupport,
                                 @Nullable NestedRoutingSupport nestedRoutingSupport) {
        this.router = router;
        this.staticRoutes = staticRoutes;
        this.ccsm = ccsm;
        this.routeFilterSupport = routeFilterSupport;
        this.nestedRoutingSupport = nestedRoutingSupport;
        var routerNodeAddresses = new HashMap<Integer, HostPort>();
        var bootstrapVirtualNodeIds = RouterContextImpl.computeBootstrapNodeIds(
                routes, nodeIdMapping, routerNodeAddresses, IntUnaryOperator.identity());
        this.infra = new RoutingInfrastructure(
                routes, nodeIdMapping, bootstrapVirtualNodeIds,
                () -> nextRoutingCorrelationId++,
                routingRequestsCounter, routingErrorsCounter,
                routingRequestDurationTimer, pendingResponseCount,
                routerNodeAddresses, IntUnaryOperator.identity());
    }

    @Nullable
    CompletionStage<RouteFilterPipeline> getOrCreateRouteFilterPipeline(
                                                                        RouteDescriptor rd,
                                                                        Channel clientChannel) {
        if (rd.filters().isEmpty() || routeFilterSupport == null) {
            return null;
        }
        return routeFilterPipelines.computeIfAbsent(rd, descriptor -> {
            var filterContext = new NettyFilterContext(clientChannel.eventLoop(), routeFilterSupport.pfr());
            List<FilterAndInvoker> filters = routeFilterSupport.filterChainFactory().createFilters(
                    filterContext, rd.filters());
            MetadataAddressCacher metadataAddressCacher = body -> {
                if (body instanceof MetadataResponseData md) {
                    for (var broker : md.brokers()) {
                        int virtualId = infra.nodeIdMapping().toVirtual(rd.name(), broker.nodeId());
                        infra.sharedNodeAddresses().put(virtualId, new HostPort(broker.host(), broker.port()));
                    }
                }
            };
            return RouteFilterPipeline.create(
                    routeFilterSupport.eventLoopGroup(),
                    clientChannel.eventLoop(), filters, clientChannel, routeFilterSupport.sniHostname(), ccsm,
                    rd.name(), infra.routingCorrelationIdAllocator(), infra.pendingResponseCount(),
                    infra.nodeIdMapping(), metadataAddressCacher);
        });
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) {
        for (var stage : routeFilterPipelines.values()) {
            stage.toCompletableFuture().thenAccept(pipeline -> {
                try {
                    pipeline.close();
                }
                catch (RuntimeException e) {
                    LOGGER.atWarn()
                            .setCause(LOGGER.isDebugEnabled() ? e : null)
                            .addKeyValue("error", e.getMessage())
                            .log(LOGGER.isDebugEnabled()
                                    ? "Failed to close route filter pipeline"
                                    : "Failed to close route filter pipeline, increase log level to DEBUG for stacktrace");
                }
            });
        }
        routeFilterPipelines.clear();
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

    /**
     * Resolves a virtual node ID to a backend address using addresses discovered from internal METADATA responses.
     */
    public Optional<HostPort> resolveRouterNodeAddress(int virtualNodeId) {
        return Optional.ofNullable(infra.sharedNodeAddresses().get(virtualNodeId));
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (msg instanceof RequestFrame frame) {
            ApiKeys apiKey = ApiKeys.forId(frame.apiKeyId());
            String staticRoute = staticRoutes.get(apiKey);
            if (staticRoute != null) {
                RouteDescriptor rd = infra.routes().get(staticRoute);
                CompletionStage<RouteFilterPipeline> pipelineStage = rd != null
                        ? getOrCreateRouteFilterPipeline(rd, ctx.channel())
                        : null;
                if (pipelineStage != null && msg instanceof DecodedRequestFrame<?> decoded) {
                    dispatchStaticWithFilters(ctx, decoded, staticRoute, apiKey, pipelineStage);
                }
                else {
                    ccsm.forwardToRoute(staticRoute, msg);
                }
                infra.routingRequestsCounter().withTags(
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

    private void dispatchStaticWithFilters(
                                           ChannelHandlerContext ctx,
                                           DecodedRequestFrame<?> decoded,
                                           String staticRoute,
                                           ApiKeys apiKey,
                                           CompletionStage<RouteFilterPipeline> pipelineStage) {
        int clientCorrelationId = decoded.correlationId();
        int routingCorrelationId = nextRoutingCorrelationId++;
        var routedFrame = new DecodedRequestFrame<>(
                decoded.apiVersion(), routingCorrelationId, decoded.decodeResponse(),
                decoded.header(), decoded.body());

        if (responseSequencer == null) {
            responseSequencer = new ResponseSequencer(ctx.channel());
        }
        long sequenceNumber = responseSequencer.allocateSequence();

        CompletableFuture<Response> future = new CompletableFuture<>();
        future.thenAccept(response -> {
            response.header().setCorrelationId(clientCorrelationId);
            var responseFrame = decoded.responseFrame(response.header(), response.body());
            responseSequencer.submit(sequenceNumber, responseFrame);
        });

        pipelineStage.thenAcceptAsync(pipeline -> {
            pipeline.writeRequest(routedFrame, future, filtered -> {
                var pendingResponse = PendingResponse.create(
                        future, staticRoute, apiKey,
                        infra.nodeIdMapping(), body -> {
                        },
                        null, pipeline);
                registerPendingResponse(ctx.channel(), routingCorrelationId, pendingResponse);
                infra.pendingResponseCount().incrementAndGet();
                ccsm.forwardToRoute(staticRoute, filtered);
            });
        }, ctx.channel().eventLoop());
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

        if (responseSequencer == null) {
            responseSequencer = new ResponseSequencer(ctx.channel());
        }

        var routingContext = new RouterContextImpl(
                frame,
                ctx.channel(),
                ccsm.sessionId(),
                ccsm.authenticatedSubject(),
                infra,
                (routeName, forwarded) -> ccsm.forwardToRoute(routeName, forwarded),
                (virtualNodeId, routeName, forwarded) -> ccsm.forwardToNode(virtualNodeId, routeName, forwarded),
                responseSequencer,
                hasNestedRouters() ? this::getOrCreateNestedRouterState : null,
                rd -> getOrCreateRouteFilterPipeline(rd, ctx.channel()));

        router.onRequest(
                apiVersion,
                apiKey,
                frame.header(),
                frame.body(),
                routingContext).whenComplete((result, error) -> {
                    if (error != null) {
                        infra.routingErrorsCounter().withTags(
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
                        handleRouterResult(routingContext, result, apiKey, correlationId);
                    }
                    ccsm.onRoutedRequestComplete();
                });
    }

    private void handleRouterResult(RouterContextImpl context,
                                    RouterResult result,
                                    ApiKeys apiKey,
                                    int correlationId) {
        if (result instanceof RouterResult.Completed completed) {
            context.submitResponse(completed.response());
            LOGGER.atTrace()
                    .addKeyValue("sessionId", ccsm.sessionId())
                    .addKeyValue("apiKey", apiKey)
                    .addKeyValue("clientCorrelationId", correlationId)
                    .log("Router completed with response");
        }
        else if (result instanceof RouterResult.CompletedNoResponse) {
            LOGGER.atTrace()
                    .addKeyValue("sessionId", ccsm.sessionId())
                    .addKeyValue("apiKey", apiKey)
                    .addKeyValue("clientCorrelationId", correlationId)
                    .log("Router completed with no response (fire-and-forget)");
        }
        else if (result instanceof RouterResult.Disconnect) {
            context.disconnectClient();
            LOGGER.atDebug()
                    .addKeyValue("sessionId", ccsm.sessionId())
                    .addKeyValue("apiKey", apiKey)
                    .addKeyValue("clientCorrelationId", correlationId)
                    .log("Router requested disconnect");
        }
    }

    private boolean hasNestedRouters() {
        return nestedRoutingSupport != null
                && infra.routes().values().stream().anyMatch(RouteDescriptor::targetsRouter);
    }

    private RouterContextImpl.NestedRouterState getOrCreateNestedRouterState(
                                                                             String routerName,
                                                                             String outerRouteName) {
        var nrs = Objects.requireNonNull(nestedRoutingSupport);
        String cacheKey = outerRouteName + ":" + routerName;
        Router nested = nestedRouters.computeIfAbsent(cacheKey,
                k -> nrs.routerChainFactory().createRouter(routerName, nrs.virtualClusterName()));

        Map<String, RouteDescriptor> nestedRoutes = nrs.allRouteDescriptors().get(routerName);
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

        IntUnaryOperator nestedTranslator = nestedVirtual -> infra.nodeIdMapping().toVirtual(outerRouteName, nestedVirtual);

        var nestedBootstrapIds = RouterContextImpl.computeBootstrapNodeIds(
                nestedRoutes, nestedNodeIdMapping, infra.sharedNodeAddresses(), nestedTranslator);

        boolean hasDeepNested = nestedRoutes.values().stream()
                .anyMatch(RouteDescriptor::targetsRouter);
        RouterContextImpl.NestedRouterProvider childProvider = hasDeepNested
                ? this::getOrCreateDeepNestedRouterState
                : null;

        return new RouterContextImpl.NestedRouterState(
                nested, nestedRoutes, nestedNodeIdMapping,
                nestedBootstrapIds, nestedTranslator, childProvider);
    }

    private RouterContextImpl.NestedRouterState getOrCreateDeepNestedRouterState(
                                                                                 String routerName,
                                                                                 String outerRouteName) {
        // Deep nesting is not yet supported; this placeholder fails clearly
        throw new UnsupportedOperationException(
                "Routing depth > 2 is not yet supported (router: " + routerName + ")");
    }

    @Override
    public boolean onResponse(Object msg) {
        if (msg instanceof DecodedResponseFrame<?> frame) {
            int correlationId = frame.correlationId();
            // Routing correlation IDs are negative (allocated from Integer.MIN_VALUE / 2 upward).
            // Non-negative IDs are normal client correlation IDs for statically-routed requests.
            if (correlationId >= 0) {
                return false;
            }
            Map<Integer, PendingResponse> pending = getPendingResponses(ccsm.clientChannel());
            PendingResponse pendingResponse = pending.remove(correlationId);
            if (pendingResponse != null) {
                infra.pendingResponseCount().decrementAndGet();
                pendingResponse.timerSample().stop(infra.routingRequestDurationTimer().withTags(
                        Metrics.ROUTE_LABEL, pendingResponse.route(),
                        Metrics.API_KEY_LABEL, pendingResponse.apiKey().name()));
                pendingResponse.metadataAddressCacher().cacheIfMetadata(frame.body());
                NodeIdResponseTranslator.translate(
                        frame.body(), frame.apiVersion(),
                        pendingResponse.nodeIdMapping(), pendingResponse.route());

                RouteFilterPipeline pipeline = pendingResponse.routeFilterPipeline();
                if (pipeline != null) {
                    if (pendingResponse.originatingRequestFrame() != null) {
                        pipeline.writeInternalResponse(
                                pendingResponse.originatingRequestFrame(),
                                (ResponseHeaderData) frame.header(),
                                frame.body());
                    }
                    else {
                        pipeline.writeResponse(
                                (ResponseHeaderData) frame.header(),
                                frame.body(),
                                correlationId,
                                frame.apiVersion());
                    }
                }
                else {
                    Response response = new ResponseImpl(
                            (ResponseHeaderData) frame.header(),
                            frame.body());
                    pendingResponse.future().complete(response);
                }
                LOGGER.atTrace()
                        .addKeyValue("sessionId", ccsm.sessionId())
                        .addKeyValue("clientCorrelationId", correlationId)
                        .log("Routed response matched to pending request");
                return true;
            }
            infra.routingErrorsCounter().withTags(
                    Metrics.ERROR_TYPE_LABEL, "unmatched_response").increment();
            LOGGER.atWarn()
                    .addKeyValue("sessionId", ccsm.sessionId())
                    .addKeyValue("clientCorrelationId", correlationId)
                    .log("Received response with no pending router future");
        }
        return false;
    }

    static void registerPendingResponse(Channel channel,
                                        int correlationId,
                                        PendingResponse pendingResponse) {
        getPendingResponses(channel).put(correlationId, pendingResponse);
    }

    static void deregisterPendingResponse(Channel channel,
                                          int correlationId) {
        getPendingResponses(channel).remove(correlationId);
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
