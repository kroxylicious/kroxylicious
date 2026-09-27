/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.router.subject;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.kafka.common.message.ApiVersionsResponseData;
import io.kroxylicious.kafka.common.message.ApiVersionsResponseData.ApiVersion;
import io.kroxylicious.kafka.common.message.RequestHeaderData;
import io.kroxylicious.kafka.common.protocol.ApiKeys;
import io.kroxylicious.kafka.common.protocol.ApiMessage;
import io.kroxylicious.kafka.common.protocol.Errors;
import io.kroxylicious.proxy.authentication.Subject;
import io.kroxylicious.proxy.router.Router;
import io.kroxylicious.proxy.router.RouterContext;
import io.kroxylicious.proxy.router.RouterResponse;
import io.kroxylicious.proxy.topology.VirtualNode;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Routes each request on a connection to the route selected for the connection's authenticated
 * subject. Per-connection state; not shared across connections.
 */
class SubjectRoutingHandler implements Router {

    private static final Logger LOGGER = LoggerFactory.getLogger(SubjectRoutingHandler.class);

    private final RouteSelector<Object> selector;
    private final RouteSelectorContext selectorContext;
    private final String virtualClusterName;
    private final String routerName;

    /**
     * Set on the first authenticated forward. {@code onRequest} is invoked serially, on the same
     * event loop thread, for a given connection, so no synchronisation is needed.
     */
    private @Nullable String pinnedRoute;

    SubjectRoutingHandler(RouteSelector<Object> selector, RouteSelectorContext selectorContext,
                          String virtualClusterName, String routerName) {
        this.selector = selector;
        this.selectorContext = selectorContext;
        this.virtualClusterName = virtualClusterName;
        this.routerName = routerName;
    }

    @Override
    public Map<ApiKeys, String> staticRoutes() {
        return Map.of(); // all dynamic
    }

    @Override
    @SuppressWarnings({ "java:S5738", "removal" })
    public CompletionStage<RouterResponse> onRequest(ApiKeys apiKey, short apiVersion,
                                                     RequestHeaderData header, ApiMessage request,
                                                     RouterContext ctx) {
        Subject subject = ctx.authenticatedSubject();
        if (subject.isAnonymous()) {
            if (apiKey == ApiKeys.API_VERSIONS) {
                return fanOutApiVersions(header, request, ctx);
            }
            return reject(ctx, header, request, "anonymous non-ApiVersions request");
        }
        return selector.selectRoute(subject, selectorContext).thenCompose(routeOpt -> {
            if (routeOpt.isEmpty()) {
                return reject(ctx, header, request, "no route for subject");
            }
            String route = routeOpt.get();
            if (pinnedRoute == null) {
                pinnedRoute = route;
            }
            else if (!pinnedRoute.equals(route)) {
                return reject(ctx, header, request, "subject route changed mid-connection from "
                        + pinnedRoute + " to " + route);
            }
            VirtualNode node = ctx.anyNode(route);
            return ctx.sendRequest(node, header, request)
                    .thenCompose(response -> ctx.respondWith(response).completed());
        }).exceptionallyCompose(err -> reject(ctx, header, request, "selector error"));
    }

    /**
     * Fans an {@code API_VERSIONS} request out to every route so an unauthenticated (pre-SASL)
     * client can negotiate a version range that every downstream cluster supports. Does not pin
     * the connection to any route.
     */
    private CompletionStage<RouterResponse> fanOutApiVersions(RequestHeaderData header, ApiMessage request, RouterContext ctx) {
        List<String> routes = List.copyOf(selectorContext.routeNames());
        List<CompletableFuture<ApiVersionsResponseData>> perRoute = routes.stream()
                .map(route -> ctx.sendRequest(ctx.anyNode(route), header.duplicate(), (ApiMessage) request.duplicate())
                        .thenApply(m -> (ApiVersionsResponseData) m)
                        .toCompletableFuture())
                .toList();
        return CompletableFuture.allOf(perRoute.toArray(CompletableFuture[]::new))
                .thenCompose(v -> {
                    List<ApiVersionsResponseData> responses = perRoute.stream().map(CompletableFuture::join).toList();
                    boolean anyError = responses.stream().anyMatch(r -> r.errorCode() != Errors.NONE.code());
                    if (anyError) {
                        return reject(ctx, header, request, "route returned an error for API_VERSIONS fan-out");
                    }
                    return ctx.respondWith(intersect(responses)).completed();
                });
    }

    /**
     * Combines per-route {@code API_VERSIONS} responses into the intersection: an API key survives
     * only if every route supports it, narrowed to the overlapping version range. Feature blocks
     * are not intersected; the first route's feature block is copied as-is.
     */
    private static ApiVersionsResponseData intersect(List<ApiVersionsResponseData> responses) {
        Map<Short, ApiVersion> merged = new LinkedHashMap<>();
        for (ApiVersion v : responses.get(0).apiKeys()) {
            merged.put(v.apiKey(), v.duplicate());
        }
        for (int i = 1; i < responses.size(); i++) {
            Map<Short, ApiVersion> thisRoute = new HashMap<>();
            for (ApiVersion v : responses.get(i).apiKeys()) {
                thisRoute.put(v.apiKey(), v);
            }
            merged.keySet().retainAll(thisRoute.keySet());
            merged.values().forEach(entry -> {
                ApiVersion other = thisRoute.get(entry.apiKey());
                entry.setMinVersion((short) Math.max(entry.minVersion(), other.minVersion()));
                entry.setMaxVersion((short) Math.min(entry.maxVersion(), other.maxVersion()));
            });
        }
        merged.values().removeIf(v -> v.minVersion() > v.maxVersion());

        ApiVersionsResponseData.ApiVersionCollection collection = new ApiVersionsResponseData.ApiVersionCollection(merged.size());
        collection.addAll(merged.values());

        ApiVersionsResponseData first = responses.get(0);
        return new ApiVersionsResponseData()
                .setErrorCode(Errors.NONE.code())
                .setThrottleTimeMs(0)
                .setApiKeys(collection)
                .setSupportedFeatures(first.supportedFeatures())
                .setFinalizedFeaturesEpoch(first.finalizedFeaturesEpoch())
                .setFinalizedFeatures(first.finalizedFeatures())
                .setZkMigrationReady(first.zkMigrationReady());
    }

    private CompletionStage<RouterResponse> reject(RouterContext ctx, RequestHeaderData header,
                                                   ApiMessage request, String reason) {
        LOGGER.atDebug()
                .addKeyValue("sessionId", ctx.sessionId())
                .addKeyValue("virtualCluster", virtualClusterName)
                .addKeyValue("router", routerName)
                .addKeyValue("reason", reason)
                .log("rejecting request");
        return ctx.respondWithError(header, request, Errors.SASL_AUTHENTICATION_FAILED)
                .withCloseConnection().completed();
    }
}
