/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.router.client;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Meter;

import io.kroxylicious.kafka.common.message.RequestHeaderData;
import io.kroxylicious.kafka.common.protocol.ApiKeys;
import io.kroxylicious.kafka.common.protocol.ApiMessage;
import io.kroxylicious.kafka.common.protocol.Errors;
import io.kroxylicious.proxy.router.Router;
import io.kroxylicious.proxy.router.RouterContext;
import io.kroxylicious.proxy.router.RouterResponse;
import io.kroxylicious.proxy.topology.VirtualNode;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Routes each request on a connection to the route selected for the connection's pinned
 * {@code client.id}. Per-connection state; not shared across connections.
 */
class ClientRoutingRouter implements Router {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClientRoutingRouter.class);

    /**
     * Bounded set of rejection reasons. Used as the {@code reason} metric tag value, so it must
     * never be derived from unbounded or sensitive input.
     */
    enum RejectReason {
        UNMAPPED("unmapped", Errors.POLICY_VIOLATION),
        ROUTE_CHANGED("route-changed", Errors.UNKNOWN_SERVER_ERROR);

        private final String tagValue;
        private final Errors errorCode;

        RejectReason(String tagValue, Errors errorCode) {
            this.tagValue = tagValue;
            this.errorCode = errorCode;
        }

        String tagValue() {
            return tagValue;
        }

        Errors errorCode() {
            return errorCode;
        }
    }

    private final List<Rule> rules;
    private final String virtualClusterName;
    private final String routerName;
    private final Meter.MeterProvider<Counter> rejectedCounter;

    /**
     * Set on the first forward. {@code onRequest} is invoked serially, on the same event loop thread,
     * for a given connection, so no synchronisation is needed.
     */
    private @Nullable String pinnedRoute;

    /**
     * Captured from the first request's header and never re-read, anticipating KIP-1313. Distinct
     * from {@link #clientIdPinned} because a client may legitimately send no {@code client.id}.
     */
    private @Nullable String pinnedClientId;
    private boolean clientIdPinned;

    ClientRoutingRouter(List<Rule> rules,
                        String virtualClusterName, String routerName) {
        this.rules = rules;
        this.virtualClusterName = virtualClusterName;
        this.routerName = routerName;
        this.rejectedCounter = ClientRouterMetrics.rejectedCounter(virtualClusterName, routerName);
    }

    @Override
    public Map<ApiKeys, String> staticRoutes() {
        return Map.of(); // all dynamic
    }

    @Override
    @SuppressWarnings({ "java:S5738" })
    public CompletionStage<RouterResponse> onRequest(ApiKeys apiKey, short apiVersion,
                                                     RequestHeaderData header, ApiMessage request,
                                                     RouterContext ctx) {
        if (!clientIdPinned) {
            pinnedClientId = header.clientId();
            clientIdPinned = true;
        }
        Optional<String> ruleRoute = resolveRoute(pinnedClientId);
        return ruleRoute.map(s -> forward(ctx, apiKey, apiVersion, header, request, s))
                .orElseGet(() -> reject(ctx, apiKey, apiVersion, header, request, RejectReason.UNMAPPED, "no rule matched clientId"));
    }

    /**
     * Pins the connection to {@code route} on the first forward; closes the connection fail-closed if
     * a later forward resolves to a different route.
     */
    private CompletionStage<RouterResponse> forward(RouterContext ctx, ApiKeys apiKey, short apiVersion,
                                                    RequestHeaderData header, ApiMessage request, String route) {
        if (pinnedRoute == null) {
            pinnedRoute = route;
        }
        else if (!pinnedRoute.equals(route)) {
            return reject(ctx, apiKey, apiVersion, header, request, RejectReason.ROUTE_CHANGED, "client route changed mid-connection from "
                    + pinnedRoute + " to " + route);
        }
        // Prefer the node the client is already connected to (a broker-specific gateway
        // endpoint) over an arbitrary node of the route, so a client that dialed a specific
        // broker keeps talking to that broker.
        VirtualNode node = ctx.virtualNode().orElseGet(() -> ctx.anyNode(route));
        return ctx.sendRequest(node, header, request)
                .thenCompose(response -> ctx.respondWith(response).completed());
    }

    /**
     * Evaluates {@code rules} in declaration order, returning the first match's route. A rule needing
     * only {@code client.id} can match before, or without, authentication.
     */
    private Optional<String> resolveRoute(@Nullable String clientId) {
        for (Rule rule : rules) {
            if (matches(clientId, rule)) {
                return Optional.of(Objects.requireNonNull(rule.useRoute()));
            }
        }
        return Optional.empty();
    }

    private static boolean matches(@Nullable String clientId, Rule rule) {
        String requiredClientId = rule.hasClientId();
        return requiredClientId == null || requiredClientId.equals(clientId);
    }

    private CompletionStage<RouterResponse> reject(RouterContext ctx, ApiKeys apiKey, short apiVersion, RequestHeaderData header,
                                                   ApiMessage request, RejectReason reason, String detail) {
        rejectedCounter.withTags(ClientRouterMetrics.REASON_LABEL, reason.tagValue()).increment();
        LOGGER.atDebug()
                .addKeyValue("sessionId", ctx.sessionId())
                .addKeyValue("virtualCluster", virtualClusterName)
                .addKeyValue("router", routerName)
                .addKeyValue("apiKey", apiKey)
                .addKeyValue("apiVersion", apiVersion)
                .addKeyValue("reason", detail)
                .log("rejecting request");
        return ctx.respondWithError(header, request, reason.errorCode())
                .withCloseConnection().completed();
    }
}
