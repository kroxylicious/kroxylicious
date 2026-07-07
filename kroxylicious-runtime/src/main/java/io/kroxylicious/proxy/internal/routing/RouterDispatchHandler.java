/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.routing;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.requests.AbstractResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.util.AttributeKey;

import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.frame.DecodedResponseFrame;
import io.kroxylicious.proxy.frame.RequestFrame;
import io.kroxylicious.proxy.internal.ClientConnectionStateMachine;
import io.kroxylicious.proxy.internal.KafkaProxyExceptionMapper;
import io.kroxylicious.proxy.router.Router;
import io.kroxylicious.proxy.service.HostPort;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Sits at the end of the VC-level filter chain (instead of
 * {@link io.kroxylicious.proxy.internal.FilterChainCompletionHandler}) when a
 * virtual cluster uses a router.
 *
 * <p>Statically-routed requests are forwarded directly to the
 * {@link ClientConnectionStateMachine}. Dynamically-routed requests are
 * deserialised and dispatched to {@link Router#onRequest}.
 *
 * <p>Implements {@link RoutingResponseCallback} so that the CCSM can deliver
 * backend responses to pending dynamic-routing futures before forwarding them
 * to the client.
 *
 * <p>The {@link #write} override applies node ID translation for statically-routed
 * API keys whose responses carry broker node IDs.
 */
public class RouterDispatchHandler extends ChannelDuplexHandler implements RoutingResponseCallback {

    private static final Logger LOGGER = LoggerFactory.getLogger(RouterDispatchHandler.class);
    private static final AttributeKey<Map<Integer, PendingResponse>> PENDING_RESPONSES = AttributeKey.valueOf(RouterDispatchHandler.class,
            "pendingResponses");

    /**
     * API keys whose responses carry node IDs that must be translated to virtual node IDs.
     * These keys are always decoded (even when statically routed) so the response bodies
     * are accessible for translation.
     */
    public static final Set<ApiKeys> NODE_ID_TRANSLATION_APIS = Set.of(
            ApiKeys.METADATA,
            ApiKeys.FIND_COORDINATOR,
            ApiKeys.DESCRIBE_CLUSTER,
            ApiKeys.PRODUCE,
            ApiKeys.FETCH,
            ApiKeys.SHARE_FETCH,
            ApiKeys.SHARE_ACKNOWLEDGE,
            ApiKeys.DESCRIBE_TOPIC_PARTITIONS);

    private final Router router;
    private final Map<String, RouteDescriptor> routes;
    private final Map<ApiKeys, String> staticRoutes;
    private final ClientConnectionStateMachine ccsm;
    private final NodeIdMapping nodeIdMapping;
    private final Map<Integer, HostPort> routerNodeAddresses = new HashMap<>();

    /**
     * Tracks correlation IDs of in-flight statically-routed requests that need response
     * node ID translation. Entries are removed when the response arrives in {@link #write}.
     */
    private final Map<Integer, String> pendingRoutes = new HashMap<>();

    private int nextRoutingCorrelationId = Integer.MIN_VALUE / 2;

    @Nullable
    private ResponseSequencer responseSequencer;

    record PendingResponse(CompletableFuture<ApiMessage> future, String route) {}

    public RouterDispatchHandler(Router router,
                                 Map<String, RouteDescriptor> routes,
                                 Map<ApiKeys, String> staticRoutes,
                                 ClientConnectionStateMachine ccsm,
                                 NodeIdMapping nodeIdMapping) {
        this.router = router;
        this.routes = routes;
        this.staticRoutes = staticRoutes;
        this.ccsm = ccsm;
        this.nodeIdMapping = nodeIdMapping;
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) {
        router.close();
    }

    /**
     * Returns the upstream address for the given virtual node ID, as learned from the most
     * recent internal METADATA response. Returns empty if the address has not been cached yet.
     */
    public Optional<HostPort> resolveRouterNodeAddress(int virtualNodeId) {
        return Optional.ofNullable(routerNodeAddresses.get(virtualNodeId));
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (msg instanceof RequestFrame frame) {
            ApiKeys apiKey = ApiKeys.forId(frame.apiKeyId());
            String staticRoute = staticRoutes.get(apiKey);
            if (staticRoute != null) {
                if (NODE_ID_TRANSLATION_APIS.contains(apiKey)) {
                    pendingRoutes.put(frame.correlationId(), staticRoute);
                }
                ccsm.forwardToRoute(staticRoute, msg);
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

        if (responseSequencer == null) {
            responseSequencer = new ResponseSequencer(ctx.channel());
        }

        var routingContext = new RouterContextImpl(
                frame,
                ctx.channel(),
                ccsm.sessionId(),
                ccsm.authenticatedSubject(),
                ccsm.nodeId(),
                routes,
                ccsm::forwardToRoute,
                ccsm::forwardToNode,
                nodeIdMapping,
                () -> nextRoutingCorrelationId++,
                responseSequencer);

        long sequence = routingContext.sequenceNumber();

        router.onRequest(apiKey, apiVersion, frame.header(), frame.body(), routingContext)
                .whenComplete((result, error) -> {
                    if (error != null) {
                        LOGGER.atError()
                                .addKeyValue("sessionId", ccsm.sessionId())
                                .addKeyValue("apiKey", apiKey)
                                .addKeyValue("clientCorrelationId", correlationId)
                                .setCause(error)
                                .log("Router returned failed future");
                        ctx.channel().close();
                        return;
                    }
                    if (!(result instanceof RouterResponseImpl rri)) {
                        LOGGER.atError()
                                .addKeyValue("sessionId", ccsm.sessionId())
                                .addKeyValue("apiKey", apiKey)
                                .addKeyValue("resultType", result == null ? "null" : result.getClass().getName())
                                .log("Router returned unrecognised RouterResponse type; closing connection");
                        ctx.channel().close();
                        return;
                    }
                    deliverResponse(ctx, rri, apiKey, apiVersion, correlationId, sequence);
                });
    }

    private void deliverResponse(ChannelHandlerContext ctx,
                                 RouterResponseImpl rri,
                                 ApiKeys apiKey,
                                 short apiVersion,
                                 int correlationId,
                                 long sequence) {
        switch (rri) {
            case RouterResponseImpl.RespondWith rw -> {
                ResponseHeaderData header = rw.header() != null ? rw.header() : new ResponseHeaderData();
                header.setCorrelationId(correlationId);
                var responseFrame = new DecodedResponseFrame<>(apiVersion, correlationId, header, rw.body());
                responseSequencer.submit(sequence, responseFrame);
            }
            case RouterResponseImpl.RespondWithError rwe -> {
                AbstractResponse errorResponse = KafkaProxyExceptionMapper.errorResponseForMessage(
                        rwe.requestHeader(), rwe.request(), rwe.exception());
                ResponseHeaderData header = new ResponseHeaderData();
                header.setCorrelationId(correlationId);
                var responseFrame = new DecodedResponseFrame<>(apiVersion, correlationId, header, errorResponse.data());
                responseSequencer.submit(sequence, responseFrame);
            }
            case RouterResponseImpl.RespondWithoutReply ignored -> LOGGER.atTrace()
                    .addKeyValue("sessionId", ccsm.sessionId())
                    .addKeyValue("apiKey", apiKey)
                    .addKeyValue("clientCorrelationId", correlationId)
                    .log("Router completed request with no reply");
        }
        if (rri.closeConnection()) {
            ctx.channel().close();
        }
        ccsm.onRoutedRequestComplete();
    }

    @Override
    public boolean onResponse(Object msg) {
        if (msg instanceof DecodedResponseFrame<?> frame) {
            int correlationId = frame.correlationId();
            // Routing correlation IDs are negative (allocated from Integer.MIN_VALUE / 2 upward).
            // Non-negative IDs belong to client requests forwarded via the normal path.
            if (correlationId >= 0) {
                return false;
            }
            Map<Integer, PendingResponse> pending = getPendingResponses(ccsm.clientChannel());
            PendingResponse pendingResponse = pending.remove(correlationId);
            if (pendingResponse != null) {
                NodeIdResponseTranslator.translate(frame.body(), frame.apiVersion(), nodeIdMapping, pendingResponse.route());
                cacheNodeAddressesIfMetadata(frame.body());
                pendingResponse.future().complete(frame.body());
                LOGGER.atTrace()
                        .addKeyValue("sessionId", ccsm.sessionId())
                        .addKeyValue("routingCorrelationId", correlationId)
                        .log("Routed response matched to pending request");
                return true;
            }
            LOGGER.atWarn()
                    .addKeyValue("sessionId", ccsm.sessionId())
                    .addKeyValue("routingCorrelationId", correlationId)
                    .log("Received response with no pending routing future");
        }
        return false;
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (msg instanceof DecodedResponseFrame<?> frame) {
            String routeName = pendingRoutes.remove(frame.correlationId());
            if (routeName != null) {
                NodeIdResponseTranslator.translate(frame.body(), frame.apiVersion(), nodeIdMapping, routeName);
            }
        }
        ctx.write(msg, promise);
    }

    static void registerPendingResponse(Channel channel, int correlationId, PendingResponse pendingResponse) {
        getPendingResponses(channel).put(correlationId, pendingResponse);
    }

    static void deregisterPendingResponse(Channel channel, int correlationId) {
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

    private void cacheNodeAddressesIfMetadata(Object body) {
        if (body instanceof MetadataResponseData md) {
            for (var broker : md.brokers()) {
                routerNodeAddresses.put(broker.nodeId(), new HostPort(broker.host(), broker.port()));
            }
            if (!md.brokers().isEmpty()) {
                LOGGER.atDebug()
                        .addKeyValue("sessionId", ccsm.sessionId())
                        .addKeyValue("brokerCount", md.brokers().size())
                        .log("Cached upstream node addresses from internal METADATA response");
            }
        }
    }
}
