/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.routing;

import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;

/**
 * A router decides which route should handle a given incoming Kafka request.
 *
 * <p>Router implementations use the {@link RoutingContext} to send requests
 * down named routes and to deliver a response back to the client. A single
 * incoming request may result in multiple outgoing requests to different
 * routes (e.g. fan-out), with the router composing the final response.</p>
 */
public interface Router {

    /**
     * Called for each incoming client request.
     *
     * <p>The implementation must use {@code context} to
     * {@linkplain RoutingContext#sendRequest send} at least one request
     * and eventually {@linkplain RoutingContext#sendResponse deliver} a
     * response to the client. The returned {@link CompletionStage} must
     * complete after the router has finished all its work for this request.</p>
     *
     * @param apiVersion the API version of the request
     * @param apiKey the API key identifying the request type
     * @param header the request header
     * @param request the request body
     * @param context the routing context for sending requests and responses
     * @return a stage that completes when the routing decision is fully handled
     */
    CompletionStage<RoutingResult> onClientRequest(
                                                   short apiVersion,
                                                   ApiKeys apiKey,
                                                   RequestHeaderData header,
                                                   ApiMessage request,
                                                   RoutingContext context);

    /**
     * Declares API keys that are always forwarded to a fixed named route
     * without deserialisation. For these API keys the runtime forwards
     * frames directly (opaque or decoded) without calling
     * {@link #onClientRequest}. API keys absent from this map are
     * considered dynamically routed and will be decoded so that
     * {@code onClientRequest} can inspect them.
     *
     * @return a map from API key to route name; empty means all API keys
     *         are dynamically routed (the default)
     */
    default Map<ApiKeys, String> staticRoutes() {
        return Map.of();
    }
}
