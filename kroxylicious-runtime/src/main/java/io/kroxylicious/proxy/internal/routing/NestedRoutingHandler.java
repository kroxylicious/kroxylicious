/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.routing;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.requests.AbstractResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;

import io.kroxylicious.proxy.authentication.Subject;
import io.kroxylicious.proxy.bootstrap.RouterChainFactory;
import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.frame.DecodedResponseFrame;
import io.kroxylicious.proxy.frame.Frame;
import io.kroxylicious.proxy.frame.RequestFrame;
import io.kroxylicious.proxy.internal.CorrelationIdAllocator;
import io.kroxylicious.proxy.internal.KafkaProxyExceptionMapper;
import io.kroxylicious.proxy.router.Router;
import io.kroxylicious.proxy.service.HostPort;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * A pipeline handler installed for each route that targets a nested router.
 * Sits after the outer route's filters and before the inner route's filters.
 *
 * <p><b>Inbound:</b> intercepts frames whose route name matches
 * {@link #activationRoute} and dispatches to the nested {@link Router}.
 * Frames for other routes pass through untouched.
 *
 * <p><b>Outbound:</b> intercepts responses whose correlation IDs match
 * pending requests sent by the nested router's {@code sendRequest()}.
 * Translates node IDs at the nested level and completes the nested future.
 * When the nested router's {@code onRequest()} completes, writes the
 * composed response upstream so it flows through outer route filters
 * back to {@link RouterDispatchHandler}.
 *
 * <p>Not thread-safe; all callers must be on the same Netty event loop.
 */
public class NestedRoutingHandler extends ChannelDuplexHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(NestedRoutingHandler.class);
    private static final String LOG_KEY_VIRTUAL_CLUSTER = "virtualCluster";
    private static final String LOG_KEY_SESSION_ID = "sessionId";
    private static final String LOG_KEY_NESTED_ROUTER = "nestedRouter";
    private static final String LOG_KEY_API_KEY = "apiKey";

    private final String activationRoute;
    private final String nestedRouterName;
    private final String virtualClusterName;
    private final RouterChainFactory routerChainFactory;
    private final Map<String, RouteDescriptor> nestedRoutes;
    private final NodeIdMapping nestedNodeIdMapping;
    private final CorrelationIdAllocator correlationIdAllocator;
    private final Map<Integer, HostPort> routerNodeAddresses;
    private final String sessionId;
    private final Subject subject;
    @Nullable
    private final Integer nodeId;

    final Map<Integer, RouterDispatchHandler.PendingResponse> pendingResponses = new HashMap<>();
    final Map<Integer, String> pendingStaticRoutes = new HashMap<>();

    @Nullable
    private Router nestedRouter;

    @Nullable
    private ChannelHandlerContext ctx;

    // all parameters are genuinely needed: identity, routing config, protocol infrastructure, session, auth, network
    @SuppressWarnings("java:S107")
    public NestedRoutingHandler(String activationRoute,
                                String nestedRouterName,
                                String virtualClusterName,
                                RouterChainFactory routerChainFactory,
                                Map<String, RouteDescriptor> nestedRoutes,
                                NodeIdMapping nestedNodeIdMapping,
                                CorrelationIdAllocator correlationIdAllocator,
                                Map<Integer, HostPort> routerNodeAddresses,
                                String sessionId,
                                Subject subject,
                                @Nullable Integer nodeId) {
        this.activationRoute = activationRoute;
        this.nestedRouterName = nestedRouterName;
        this.virtualClusterName = virtualClusterName;
        this.routerChainFactory = routerChainFactory;
        this.nestedRoutes = nestedRoutes;
        this.nestedNodeIdMapping = nestedNodeIdMapping;
        this.correlationIdAllocator = correlationIdAllocator;
        this.routerNodeAddresses = routerNodeAddresses;
        this.sessionId = sessionId;
        this.subject = subject;
        this.nodeId = nodeId;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        this.ctx = ctx;
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) {
        if (!pendingResponses.isEmpty()) {
            var cause = new IllegalStateException("Connection closed with " + pendingResponses.size()
                    + " pending nested router response(s)");
            for (var entry : pendingResponses.values()) {
                entry.future().completeExceptionally(cause);
            }
            pendingResponses.clear();
        }
        if (nestedRouter != null) {
            nestedRouter.close();
            nestedRouter = null;
        }
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof DecodedRequestFrame<?> frame && activationRoute.equals(frame.routeName())) {
            dispatchToNestedRouter(ctx, frame);
            return;
        }
        if (msg instanceof RequestFrame rf && !(msg instanceof DecodedRequestFrame<?>)
                && activationRoute.equals(rf.routeName())) {
            handleOpaqueFrame(ctx, rf, msg);
            return;
        }
        ctx.fireChannelRead(msg);
    }

    private void handleOpaqueFrame(ChannelHandlerContext ctx, RequestFrame rf, Object msg) {
        ApiKeys apiKey = ApiKeys.forId(rf.apiKeyId());
        if (nestedRouter == null) {
            nestedRouter = routerChainFactory.createRouter(nestedRouterName, virtualClusterName);
        }
        String staticRoute = nestedRouter.staticRoutes().get(apiKey);
        if (staticRoute != null) {
            String qualifiedRoute = nestedRouterName + "/" + staticRoute;
            ((Frame) msg).setRouteName(qualifiedRoute);
            ctx.fireChannelRead(msg);
            LOGGER.atTrace()
                    .addKeyValue(LOG_KEY_VIRTUAL_CLUSTER, virtualClusterName)
                    .addKeyValue(LOG_KEY_SESSION_ID, sessionId)
                    .addKeyValue(LOG_KEY_NESTED_ROUTER, nestedRouterName)
                    .addKeyValue(LOG_KEY_API_KEY, apiKey)
                    .addKeyValue("route", qualifiedRoute)
                    .log("Nested static route selected for opaque frame");
            return;
        }
        LOGGER.atWarn()
                .addKeyValue(LOG_KEY_VIRTUAL_CLUSTER, virtualClusterName)
                .addKeyValue(LOG_KEY_SESSION_ID, sessionId)
                .addKeyValue(LOG_KEY_NESTED_ROUTER, nestedRouterName)
                .addKeyValue(LOG_KEY_API_KEY, apiKey)
                .log("Opaque frame arrived for dynamically-routed nested API key; decode predicate misconfigured");
        ctx.close();
    }

    private void dispatchToNestedRouter(ChannelHandlerContext ctx, DecodedRequestFrame<?> frame) {
        int outerCorrelationId = frame.correlationId();
        ApiKeys apiKey = frame.apiKey();
        short apiVersion = frame.apiVersion();

        LOGGER.atTrace()
                .addKeyValue(LOG_KEY_VIRTUAL_CLUSTER, virtualClusterName)
                .addKeyValue(LOG_KEY_SESSION_ID, sessionId)
                .addKeyValue(LOG_KEY_NESTED_ROUTER, nestedRouterName)
                .addKeyValue(LOG_KEY_API_KEY, apiKey)
                .addKeyValue("outerCorrelationId", outerCorrelationId)
                .log("Dispatching to nested router");

        if (nestedRouter == null) {
            nestedRouter = routerChainFactory.createRouter(nestedRouterName, virtualClusterName);
        }

        Map<ApiKeys, String> nestedStaticRoutes = nestedRouter.staticRoutes();
        String staticRoute = nestedStaticRoutes.get(apiKey);
        if (staticRoute != null) {
            String qualifiedRoute = nestedRouterName + "/" + staticRoute;
            frame.setRouteName(qualifiedRoute);
            if (RouterDispatchHandler.NODE_ID_TRANSLATION_APIS.contains(apiKey)) {
                pendingStaticRoutes.put(outerCorrelationId, staticRoute);
            }
            ctx.fireChannelRead(frame);
            LOGGER.atTrace()
                    .addKeyValue(LOG_KEY_VIRTUAL_CLUSTER, virtualClusterName)
                    .addKeyValue(LOG_KEY_SESSION_ID, sessionId)
                    .addKeyValue(LOG_KEY_NESTED_ROUTER, nestedRouterName)
                    .addKeyValue(LOG_KEY_API_KEY, apiKey)
                    .addKeyValue("route", qualifiedRoute)
                    .log("Nested static route selected");
            return;
        }

        var nestedDispatch = new NestedRouterDispatch(
                nestedRoutes,
                nestedNodeIdMapping,
                nestedRouterName,
                correlationIdAllocator,
                pendingResponses,
                ctx);

        var nestedCtx = new RouterContextImpl(
                frame,
                nestedDispatch,
                sessionId,
                subject,
                nodeId);

        nestedRouter.onRequest(apiKey, apiVersion, frame.header(), frame.body(), nestedCtx)
                .whenComplete((result, error) -> {
                    if (error != null) {
                        LOGGER.atError()
                                .addKeyValue(LOG_KEY_VIRTUAL_CLUSTER, virtualClusterName)
                                .addKeyValue(LOG_KEY_SESSION_ID, sessionId)
                                .addKeyValue(LOG_KEY_NESTED_ROUTER, nestedRouterName)
                                .addKeyValue(LOG_KEY_API_KEY, apiKey)
                                .setCause(error)
                                .log("Nested router returned failed future");
                        writeErrorResponse(ctx, frame, error);
                        return;
                    }
                    if (!(result instanceof RouterResponseImpl rri)) {
                        LOGGER.atError()
                                .addKeyValue(LOG_KEY_VIRTUAL_CLUSTER, virtualClusterName)
                                .addKeyValue(LOG_KEY_SESSION_ID, sessionId)
                                .addKeyValue(LOG_KEY_NESTED_ROUTER, nestedRouterName)
                                .log("Nested router returned unrecognised RouterResponse type");
                        writeErrorResponse(ctx, frame,
                                new IllegalStateException("Nested router returned unrecognised response type"));
                        return;
                    }
                    if (rri.closeConnection()) {
                        LOGGER.atWarn()
                                .addKeyValue(LOG_KEY_VIRTUAL_CLUSTER, virtualClusterName)
                                .addKeyValue(LOG_KEY_SESSION_ID, sessionId)
                                .addKeyValue(LOG_KEY_NESTED_ROUTER, nestedRouterName)
                                .log("Nested router attempted to close connection; ignoring close request");
                    }
                    writeNestedResponse(ctx, frame, rri);
                });
    }

    private void writeNestedResponse(ChannelHandlerContext ctx,
                                     DecodedRequestFrame<?> requestFrame,
                                     RouterResponseImpl rri) {
        ApiMessage body;
        switch (rri) {
            case RouterResponseImpl.RespondWith rw -> body = rw.body();
            case RouterResponseImpl.RespondWithError rwe -> {
                AbstractResponse errorResponse = KafkaProxyExceptionMapper.errorResponseForMessage(
                        rwe.requestHeader(), rwe.request(), rwe.exception());
                body = errorResponse.data();
            }
            case RouterResponseImpl.RespondWithoutReply ignored -> {
                LOGGER.atTrace()
                        .addKeyValue(LOG_KEY_VIRTUAL_CLUSTER, virtualClusterName)
                        .addKeyValue(LOG_KEY_SESSION_ID, sessionId)
                        .addKeyValue(LOG_KEY_NESTED_ROUTER, nestedRouterName)
                        .addKeyValue("outerCorrelationId", requestFrame.correlationId())
                        .log("Nested router completed with no reply");
                return;
            }
        }
        var header = new ResponseHeaderData();
        header.setCorrelationId(requestFrame.correlationId());
        var responseFrame = requestFrame.responseFrame(header, body);
        responseFrame.setRouteName(activationRoute);
        ctx.write(responseFrame, ctx.voidPromise());
        ctx.flush();
    }

    private void writeErrorResponse(ChannelHandlerContext ctx,
                                    DecodedRequestFrame<?> requestFrame,
                                    Throwable error) {
        var header = new ResponseHeaderData();
        header.setCorrelationId(requestFrame.correlationId());
        ApiMessage body = KafkaProxyExceptionMapper.errorResponseForMessage(
                requestFrame.header(), requestFrame.body(), new UnknownServerException(error.getMessage())).data();
        var responseFrame = requestFrame.responseFrame(header, body);
        responseFrame.setRouteName(activationRoute);
        ctx.write(responseFrame, ctx.voidPromise());
        ctx.flush();
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (msg instanceof DecodedResponseFrame<?> frame) {
            int correlationId = frame.correlationId();
            RouterDispatchHandler.PendingResponse pending = pendingResponses.remove(correlationId);
            if (pending != null) {
                NodeIdResponseTranslator.translate(frame.body(), frame.apiVersion(),
                        pending.nodeIdMapping(), pending.route());
                cacheNodeAddressesIfMetadata(frame.body());
                pending.future().complete(frame.body());
                frame.release();
                LOGGER.atTrace()
                        .addKeyValue(LOG_KEY_VIRTUAL_CLUSTER, virtualClusterName)
                        .addKeyValue(LOG_KEY_SESSION_ID, sessionId)
                        .addKeyValue(LOG_KEY_NESTED_ROUTER, nestedRouterName)
                        .addKeyValue("routingCorrelationId", correlationId)
                        .log("Nested routed response matched to pending request");
                promise.setSuccess();
                return;
            }
            String staticRoute = pendingStaticRoutes.remove(correlationId);
            if (staticRoute != null) {
                NodeIdResponseTranslator.translate(frame.body(), frame.apiVersion(),
                        nestedNodeIdMapping, staticRoute);
            }
        }
        ctx.write(msg, promise);
    }

    private void cacheNodeAddressesIfMetadata(Object body) {
        if (body instanceof MetadataResponseData md) {
            for (var broker : md.brokers()) {
                routerNodeAddresses.put(broker.nodeId(), new HostPort(broker.host(), broker.port()));
            }
        }
    }
}
