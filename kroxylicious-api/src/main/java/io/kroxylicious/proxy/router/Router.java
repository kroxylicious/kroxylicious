/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.router;

import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;

/**
 * A router decides which route should handle a given incoming Kafka request.
 *
 * <p>Router implementations use the {@link RouterContext} to send requests
 * down named routes and to deliver a response back to the client. A single
 * incoming request may result in multiple outgoing requests to different
 * routes (e.g. fan-out), with the router composing the final response.</p>
 *
 * <h2>Control plane vs data plane</h2>
 *
 * <p>Kafka makes no protocol distinction between a bootstrap broker and a data
 * broker, but the router world must, because the connection's endpoint is the
 * only signal for whether a default broker exists:</p>
 * <ul>
 *   <li><strong>Bootstrap endpoint</strong> → connection is <em>unbound</em>
 *       ({@link RouterContext#virtualNode()} is empty) → the <strong>control
 *       plane</strong>: discovery, {@code METADATA}, coordinator lookup, the
 *       routing decisions.</li>
 *   <li><strong>Broker endpoint</strong> → connection is <em>bound</em> to one
 *       {@code (route, broker)} → the <strong>data plane</strong>: data-plane
 *       traffic to a known broker.</li>
 * </ul>
 *
 * <p>By default, a router intercepts only control-plane traffic (bootstrap
 * connections where {@code virtualNode()} is empty). Data-plane traffic on
 * bound connections passes through to the assigned broker without calling
 * {@link #onRequest}. Routers override {@link #shouldIntercept} to declare
 * additional API keys they need to intercept.</p>
 *
 * <h2>Observability guidelines for router implementations</h2>
 *
 * <p>The runtime automatically logs and measures the following on behalf of
 * all router implementations:</p>
 * <ul>
 *   <li>Which route each request was sent to (at TRACE level, with {@code route} key)</li>
 *   <li>Request/response correlation</li>
 *   <li>Per-route request counts, error counts, and latency (as Micrometer metrics)</li>
 *   <li>Error conditions such as unknown routes and router failures</li>
 * </ul>
 *
 * <p>Router implementations should <strong>not</strong> duplicate the above.
 * Instead, implementations should log:</p>
 * <ul>
 *   <li><strong>Routing rationale at DEBUG:</strong> explain <em>why</em> a
 *       particular route was chosen when the logic is non-trivial. Always
 *       include {@link RouterContext#sessionId()} for correlation with
 *       runtime logs.</li>
 *   <li><strong>Configuration at INFO during initialisation:</strong> log once
 *       from {@link RouterFactory#createRouter} to describe the router's
 *       configuration.</li>
 *   <li><strong>Response mutation at DEBUG:</strong> if the router modifies
 *       responses (e.g. version capping in {@code API_VERSIONS}), log the
 *       modification since it changes protocol behaviour visible to
 *       clients.</li>
 *   <li><strong>Recovered errors at WARN:</strong> if the router catches
 *       exceptions internally and recovers, log them with conditional stack
 *       traces (include the full stack trace only when DEBUG is enabled).</li>
 * </ul>
 *
 * <p>Router implementations <strong>must not</strong>:</p>
 * <ul>
 *   <li>Log Kafka message content (may contain sensitive data).</li>
 *   <li>Log at INFO or above on every request (reserve INFO+ for lifecycle
 *       events; per-request logging at that level causes excessive volume
 *       in production).</li>
 * </ul>
 */
public interface Router {

    /**
     * Called for each incoming client request that is dynamically routed.
     *
     * <p>The implementation inspects the request, sends one or more requests
     * via {@link RouterContext#sendRequest}, and returns a
     * {@link RouterResponse} encoding the outcome. Use the builder methods on
     * {@link RouterContext} to construct results:
     * {@link RouterContext#respondWith(ApiMessage) respondWith} to deliver a
     * response, {@link RouterContext#respondWithoutReply() respondWithoutReply}
     * for acks=0 {@code Produce} requests, or
     * {@link RouterContext#respondWithError respondWithError} to generate an
     * error response.</p>
     *
     * <p><strong>Threading model</strong></p>
     *
     * <p>All invocations of this method, all calls to
     * {@link RouterContext#sendRequest}, and all
     * {@link CompletionStage} callbacks chained on the futures returned
     * by {@code sendRequest}, execute on the same Netty event loop
     * thread. Router implementations do not need to synchronise access
     * to their own state.</p>
     *
     * @param apiKey the API key identifying the request type
     * @param apiVersion the API version of the request
     * @param header the request header
     * @param request the request body
     * @param context the router context for sending requests
     * @return a stage that completes with the routing outcome
     */
    CompletionStage<RouterResponse> onRequest(ApiKeys apiKey,
                                              short apiVersion,
                                              RequestHeaderData header,
                                              ApiMessage request,
                                              RouterContext context);

    /**
     * Called by the runtime when the client connection is torn down.
     *
     * <p>Implementations should release any per-connection resources
     * (e.g. reclaim cache slots).</p>
     *
     * <p>Guaranteed to be called on the same event loop thread as
     * {@link #onRequest}. Called at most once per router instance.</p>
     */
    default void close() {
    }

    /**
     * Whether the router must be invoked for this request. When false the
     * runtime forwards the frame to the connection's assigned broker
     * ({@link RouterContext#virtualNode()}) without calling
     * {@link #onRequest}.
     *
     * <p>The default implementation intercepts only when there is no assigned
     * broker (bootstrap connections where {@code virtualNode()} is empty).
     * This allows data-plane traffic on bound connections to pass through
     * without decoding or routing decisions.</p>
     *
     * <p><strong>Common patterns:</strong></p>
     * <ul>
     *   <li><strong>Single-cluster router</strong>: intercept only on bootstrap
     *       ({@code virtualNode().isEmpty()}) to assign the cluster, then pass
     *       through on bound connections (the default behaviour).</li>
     *   <li><strong>Client-id or subject router</strong>: intercept only on
     *       bootstrap ({@code virtualNode().isEmpty()}) to make the routing
     *       decision, then pass through on bound connections.</li>
     *   <li><strong>Topic router</strong>: intercept on bootstrap plus
     *       cluster-spanning APIs ({@code METADATA}, {@code FIND_COORDINATOR})
     *       and coordinator-pinned APIs on bound connections.</li>
     * </ul>
     *
     * <p><strong>Threading model:</strong> Called on the same Netty event loop
     * thread as {@link #onRequest}. Router implementations do not need to
     * synchronise access to their own state.</p>
     *
     * @param apiKey the API key identifying the request type
     * @param apiVersion the API version of the request
     * @param context the router context providing connection state
     * @return true if the router needs to handle this request via
     *         {@link #onRequest}, false to forward directly to the assigned
     *         broker
     */
    default boolean shouldIntercept(ApiKeys apiKey, short apiVersion, RouterContext context) {
        return context.virtualNode().isEmpty();
    }
}
