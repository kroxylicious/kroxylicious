/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.routing;

import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.spi.LoggingEventBuilder;

import io.netty.channel.ChannelHandlerContext;

import io.kroxylicious.kafka.common.message.MetadataResponseData;
import io.kroxylicious.kafka.common.message.RequestHeaderData;
import io.kroxylicious.kafka.common.protocol.ApiKeys;
import io.kroxylicious.kafka.common.protocol.ApiMessage;
import io.kroxylicious.proxy.frame.DecodedResponseFrame;
import io.kroxylicious.proxy.frame.PathElement;
import io.kroxylicious.proxy.internal.CorrelationIdAllocator;
import io.kroxylicious.proxy.internal.InternalRequestFrame;
import io.kroxylicious.proxy.service.HostPort;
import io.kroxylicious.proxy.tag.VisibleForTesting;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Creates request frames and fires them downstream, and matches returning response frames back
 * to their pending futures — translating node IDs and caching upstream broker addresses along
 * the way.
 *
 * <p>Parameterised by the path leading to this dispatcher's own router (empty/{@code null} for
 * the top-level router, the activating route's path for a nested router) so the same logic serves
 * every router in the routing tree, and so each level's out-of-band ({@link RouterContextImpl#sendRequest})
 * requests carry a path distinct from every other level's — recognised on the way back by exact
 * structural match, with no id-keyed lookup table required.
 *
 * <p>Not thread-safe; all callers must be on the same Netty event loop. Off-loop calls are
 * bridged via {@link #executeOnEventLoop}.
 */
public class RouteDispatcher implements RouterDispatch {

    private static final Logger LOGGER = LoggerFactory.getLogger(RouteDispatcher.class);

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

    private static final String LOG_KEY_ROUTING_CORRELATION_ID = "correlationId";
    private static final String LOG_KEY_TARGET_NODE_ID = "targetNodeId";
    private static final String LOG_KEY_VIRTUAL_CLUSTER = "virtualCluster";
    private static final String LOG_KEY_SESSION_ID = "sessionId";

    private final Map<String, RouteDescriptor> routes;
    private final NodeIdMapping nodeIdMapping;
    /**
     * Prefix applied to a local route name to match {@code VirtualClusterModel}'s config-level
     * route-to-cluster lookup keys: empty for the top-level router (whose routes use unqualified
     * names), {@code "<routerName>/"} for a nested router. This is a one-hop, config-key concern,
     * independent of {@link #parentPath}'s full-lineage accumulation.
     */
    private final String qualificationPrefix;
    @Nullable
    private final PathElement.Route parentPath;
    private final CorrelationIdAllocator correlationIdAllocator;
    private final Map<Integer, HostPort> routerNodeAddresses;
    private final String virtualClusterName;

    private final Map<Integer, String> pendingStaticRoutes = new HashMap<>();

    /**
     * Outstanding promises for requests this dispatcher has sent, tracked only so they can be
     * failed if the connection closes before a response arrives - not used for matching (that's
     * done structurally via each response's own path, see {@link #handleResponse}).
     */
    private final Set<CompletableFuture<ApiMessage>> pendingPromises = Collections.newSetFromMap(new IdentityHashMap<>());

    @Nullable
    private ChannelHandlerContext ctx;

    enum ResponseOutcome {
        CONSUMED,
        STATIC_TRANSLATED,
        UNHANDLED
    }

    RouteDispatcher(Map<String, RouteDescriptor> routes,
                    NodeIdMapping nodeIdMapping,
                    String qualificationPrefix,
                    @Nullable PathElement.Route parentPath,
                    CorrelationIdAllocator correlationIdAllocator,
                    Map<Integer, HostPort> routerNodeAddresses,
                    String virtualClusterName) {
        this.routes = routes;
        this.nodeIdMapping = nodeIdMapping;
        this.qualificationPrefix = qualificationPrefix;
        this.parentPath = parentPath;
        this.correlationIdAllocator = correlationIdAllocator;
        this.routerNodeAddresses = routerNodeAddresses;
        this.virtualClusterName = virtualClusterName;
    }

    void setContext(ChannelHandlerContext ctx) {
        this.ctx = ctx;
    }

    @Override
    public Map<String, RouteDescriptor> routes() {
        return routes;
    }

    @Override
    public NodeIdMapping nodeIdMapping() {
        return nodeIdMapping;
    }

    /**
     * Returns the correlation ID allocator used to mint ids for out-of-band requests this
     * dispatcher issues. Shared connection-wide; see {@code ClientConnectionStateMachine}.
     *
     * @return the correlation ID allocator
     */
    public CorrelationIdAllocator correlationIdAllocator() {
        return correlationIdAllocator;
    }

    /**
     * Returns the upstream node addresses known at this routing level, keyed by virtual node ID.
     *
     * @return the known upstream node addresses
     */
    public Map<Integer, HostPort> routerNodeAddresses() {
        return routerNodeAddresses;
    }

    /**
     * Returns the upstream address for the given virtual node ID, as learned from the most
     * recent internal METADATA response. Returns empty if the address has not been cached yet.
     *
     * @param virtualNodeId the virtual node ID to resolve
     * @return the upstream address of the node, or empty if not yet known
     */
    public Optional<HostPort> resolveRouterNodeAddress(int virtualNodeId) {
        return Optional.ofNullable(routerNodeAddresses.get(virtualNodeId));
    }

    /**
     * Builds the path for a request/response on the given local route name, at this dispatcher's
     * own nesting level. The route element's name is qualified to match
     * {@code VirtualClusterModel}'s config-level route-to-cluster lookup keys (see
     * {@link #qualificationPrefix}); {@link PathElement#parent()} carries the full ancestor lineage.
     *
     * @param route the local (unqualified) route name
     * @return the path
     */
    PathElement.Route routePathFor(String route) {
        return new PathElement.Route(qualificationPrefix + route, parentPath);
    }

    /**
     * Recovers the local (unqualified) route name from one of this dispatcher's own route
     * elements - the inverse of {@link #routePathFor(String)} - for looking up
     * {@link RouteDescriptor}s and {@link NodeIdMapping} entries, which are keyed by local name.
     */
    private String localRouteName(PathElement.Route route) {
        return route.name().substring(qualificationPrefix.length());
    }

    void trackStaticRoute(int correlationId, String route) {
        pendingStaticRoutes.put(correlationId, route);
    }

    // --- Dispatch ---

    @Override
    public CompletionStage<ApiMessage> sendToAnyNode(String route,
                                                     RequestHeaderData header,
                                                     ApiMessage request,
                                                     String sessionId,
                                                     int clientCorrelationId) {
        return executeOnEventLoop(() -> doSendToAnyNode(route, header, request, sessionId, clientCorrelationId));
    }

    @Override
    public CompletionStage<ApiMessage> sendToSpecificNode(int targetNodeId,
                                                          String route,
                                                          RequestHeaderData header,
                                                          ApiMessage request,
                                                          String sessionId,
                                                          int clientCorrelationId) {
        return executeOnEventLoop(() -> doSendToSpecificNode(targetNodeId, route, header, request, sessionId, clientCorrelationId));
    }

    private CompletableFuture<ApiMessage> doSendToAnyNode(String route, RequestHeaderData header, ApiMessage request, String sessionId,
                                                          int clientCorrelationId) {
        RouteDescriptor rd = routes.get(route);
        if (rd == null) {
            withSendContext(LOGGER.atWarn(), sessionId, route, clientCorrelationId)
                    .log("Router attempted to send to unknown route");
            return CompletableFuture.failedFuture(new IllegalArgumentException("Unknown route: " + route));
        }
        return sendInternal(route, header, request, null, () -> withSendContext(LOGGER.atTrace(), sessionId, route, clientCorrelationId));
    }

    private CompletableFuture<ApiMessage> doSendToSpecificNode(int targetNodeId,
                                                               String route,
                                                               RequestHeaderData header,
                                                               ApiMessage request,
                                                               String sessionId,
                                                               int clientCorrelationId) {
        RouteDescriptor rd = routes.get(route);
        if (rd == null) {
            withNodeContext(LOGGER.atWarn(), sessionId, route, targetNodeId)
                    .log("Target node resolved to unknown route");
            return CompletableFuture.failedFuture(
                    new IllegalStateException("Node " + targetNodeId + " resolved to unknown route: " + route));
        }
        return sendInternal(route, header, request, targetNodeId, () -> withNodeContext(LOGGER.atTrace(), sessionId, route, targetNodeId));
    }

    private CompletableFuture<ApiMessage> sendInternal(String route,
                                                       RequestHeaderData header,
                                                       ApiMessage request,
                                                       @Nullable Integer targetNodeId,
                                                       Supplier<LoggingEventBuilder> logContext) {
        short requestApiVersion = header.requestApiVersion();
        // Distinct per request so plugin code (e.g. a route filter observing this traffic via
        // onRequest/onResponse) that keys its own bookkeeping off correlation id doesn't collide
        // when multiple such requests are in flight concurrently - matching back to the right
        // promise never reads this value, it's carried on the frame's path instead.
        int correlationId = correlationIdAllocator.allocateId();
        header.setCorrelationId(correlationId);
        var routePath = routePathFor(route);

        var probeFrame = new InternalRequestFrame<>(requestApiVersion, correlationId, true, header, request);
        if (targetNodeId != null) {
            probeFrame.setTargetVirtualNodeId(targetNodeId);
        }

        if (!probeFrame.hasResponse()) {
            probeFrame.setPath(routePath);
            fireChannelRead(probeFrame);
            logContext.get()
                    .addKeyValue(LOG_KEY_ROUTING_CORRELATION_ID, correlationId)
                    .log("Fire-and-forget request sent (no response expected)");
            return CompletableFuture.completedFuture(null);
        }

        CompletableFuture<ApiMessage> future = new CompletableFuture<>();
        pendingPromises.add(future);
        probeFrame.setPath(new PathElement.RouterOrigin(future, routePath));
        fireChannelRead(probeFrame);
        logContext.get()
                .addKeyValue(LOG_KEY_ROUTING_CORRELATION_ID, correlationId)
                .addKeyValue("apiVersion", requestApiVersion)
                .log("Request sent");
        return future;
    }

    // --- Response correlation ---

    ResponseOutcome handleResponse(DecodedResponseFrame<?> frame, String sessionId) {
        PathElement path = frame.path();
        if (path instanceof PathElement.RouterOrigin router
                && router.parent() != null
                && Objects.equals(router.parent().parent(), parentPath)) {
            PathElement.Route routeSegment = router.parent();
            @SuppressWarnings("unchecked")
            CompletableFuture<ApiMessage> future = (CompletableFuture<ApiMessage>) router.promise();
            pendingPromises.remove(future);
            ApiMessage body = frame.body();
            try {
                NodeIdResponseTranslator.translate(body, frame.apiVersion(), nodeIdMapping, localRouteName(routeSegment));
                cacheNodeAddressesIfMetadata(body, sessionId);
                future.complete(body);
            }
            catch (Exception t) {
                future.completeExceptionally(t);
            }
            finally {
                // Safe: DecodedResponseFrames decoded from the network carry no managed ByteBufs
                // (KafkaResponseDecoder adds none), and RouteFilterHandler only intercepts frames
                // whose path lies on its own route — upstream responses not on any route pass
                // through unmodified. So body does not alias any ByteBuf released here.
                frame.release();
            }
            LOGGER.atTrace()
                    .addKeyValue(LOG_KEY_VIRTUAL_CLUSTER, virtualClusterName)
                    .addKeyValue(LOG_KEY_SESSION_ID, sessionId)
                    .addKeyValue("route", routeSegment.name())
                    .log("Routed response matched to pending request");
            return ResponseOutcome.CONSUMED;
        }

        String staticRoute = pendingStaticRoutes.remove(frame.correlationId());
        if (staticRoute != null) {
            NodeIdResponseTranslator.translate(frame.body(), frame.apiVersion(),
                    nodeIdMapping, staticRoute);
            cacheNodeAddressesIfMetadata(frame.body(), sessionId);
            return ResponseOutcome.STATIC_TRANSLATED;
        }

        return ResponseOutcome.UNHANDLED;
    }

    @VisibleForTesting
    void addPendingStaticRoute(int correlationId, String route) {
        pendingStaticRoutes.put(correlationId, route);
    }

    @VisibleForTesting
    boolean hasPendingStaticRoute(int correlationId) {
        return pendingStaticRoutes.containsKey(correlationId);
    }

    // --- Lifecycle ---

    void failAllPending(String sessionId) {
        int abandoned = pendingPromises.size();
        if (abandoned > 0) {
            var cause = new IllegalStateException("Connection closed with " + abandoned + " pending router response(s)");
            for (var future : pendingPromises) {
                future.completeExceptionally(cause);
            }
            pendingPromises.clear();
            LOGGER.atWarn()
                    .addKeyValue(LOG_KEY_VIRTUAL_CLUSTER, virtualClusterName)
                    .addKeyValue(LOG_KEY_SESSION_ID, sessionId)
                    .addKeyValue("abandonedResponses", abandoned)
                    .log("Connection closed with pending router responses");
        }
    }

    // --- Internal helpers ---

    // FutureReturnValueIgnored: a synchronous throw from work.get() is caught and
    // propagated to `bridge`, so the submitted task cannot complete exceptionally and the
    // whenComplete callback completes `bridge` in both branches.
    @SuppressWarnings("FutureReturnValueIgnored")
    private <T> CompletionStage<T> executeOnEventLoop(Supplier<CompletableFuture<T>> work) {
        var executor = Objects.requireNonNull(ctx, "sendRequest called before handlerAdded").executor();
        if (executor.inEventLoop()) {
            return work.get();
        }
        CompletableFuture<T> bridge = new CompletableFuture<>();
        executor.execute(() -> {
            try {
                work.get().whenComplete((r, e) -> {
                    if (e != null) {
                        bridge.completeExceptionally(e);
                    }
                    else {
                        bridge.complete(r);
                    }
                });
            }
            catch (Exception t) {
                bridge.completeExceptionally(t);
            }
        });
        return bridge;
    }

    private void fireChannelRead(Object msg) {
        Objects.requireNonNull(ctx, "fireChannelRead called before handlerAdded").fireChannelRead(msg);
    }

    private void cacheNodeAddressesIfMetadata(Object body, String sessionId) {
        if (body instanceof MetadataResponseData md) {
            for (var broker : md.brokers()) {
                routerNodeAddresses.put(broker.nodeId(), new HostPort(broker.host(), broker.port()));
            }
            if (!md.brokers().isEmpty()) {
                LOGGER.atDebug()
                        .addKeyValue(LOG_KEY_VIRTUAL_CLUSTER, virtualClusterName)
                        .addKeyValue(LOG_KEY_SESSION_ID, sessionId)
                        .addKeyValue("brokerCount", md.brokers().size())
                        .log("Cached upstream node addresses from internal METADATA response");
            }
        }
    }

    private LoggingEventBuilder withSendContext(LoggingEventBuilder event, String sessionId, String route, int clientCorrelationId) {
        return event.addKeyValue(LOG_KEY_VIRTUAL_CLUSTER, virtualClusterName)
                .addKeyValue(LOG_KEY_SESSION_ID, sessionId)
                .addKeyValue("route", route)
                .addKeyValue("clientCorrelationId", clientCorrelationId);
    }

    private LoggingEventBuilder withNodeContext(LoggingEventBuilder event, String sessionId, String route, int targetNodeId) {
        return event.addKeyValue(LOG_KEY_VIRTUAL_CLUSTER, virtualClusterName)
                .addKeyValue(LOG_KEY_SESSION_ID, sessionId)
                .addKeyValue(LOG_KEY_TARGET_NODE_ID, targetNodeId)
                .addKeyValue("route", route);
    }
}
