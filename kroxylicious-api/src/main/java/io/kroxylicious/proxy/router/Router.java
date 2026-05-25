/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.router;

import java.util.Map;
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
     * via {@link RouterContext#sendRequestToNode}, and returns a
     * {@link RouterResult} encoding the outcome. Use
     * {@link RouterResult.Completed Completed} to deliver a response,
     * {@link RouterResult.CompletedNoResponse CompletedNoResponse} for
     * acks=0 {@code Produce} requests, or {@link RouterResult.Disconnect Disconnect}
     * to close the client connection.</p>
     *
     * <p><strong>Threading model</strong></p>
     *
     * <p>All invocations of this method, all calls to
     * {@link RouterContext#sendRequestToNode}, and all
     * {@link CompletionStage} callbacks chained on the futures returned
     * by {@code sendRequestToNode}, execute on the same Netty event loop
     * thread. Router implementations do not need to synchronise access
     * to their own state.</p>
     *
     * @param apiVersion the API version of the request
     * @param apiKey the API key identifying the request type
     * @param header the request header
     * @param request the request body
     * @param context the router context for sending requests
     * @return a stage that completes with the routing outcome
     */
    CompletionStage<RouterResult> onRequest(
                                            short apiVersion,
                                            ApiKeys apiKey,
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
     * Declares API keys that are always forwarded to a fixed named route
     * without deserialisation. For these API keys the runtime forwards
     * frames directly (opaque or decoded) without calling
     * {@link #onRequest}. API keys absent from this map are
     * considered dynamically routed and will be decoded so that
     * {@code onRequest} can inspect them.
     *
     * @return a map from API key to route name; empty means all API keys
     *         are dynamically routed (the default)
     */
    default Map<ApiKeys, String> staticRoutes() {
        return Map.of();
    }
}
