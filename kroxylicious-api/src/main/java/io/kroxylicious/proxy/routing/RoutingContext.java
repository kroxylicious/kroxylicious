/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.routing;

import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.ApiMessage;

import io.kroxylicious.proxy.authentication.Subject;

/**
 * Context passed to {@link Router#onClientRequest} for issuing requests
 * to named routes and delivering responses to the client.
 */
public interface RoutingContext {

    /**
     * Sends a request down the named route.
     *
     * <p>The request will pass through any filters configured on the route
     * before reaching the route's receiver (a backing cluster or another
     * router). The returned stage completes when the receiver produces a
     * response.</p>
     *
     * @param route the name of the route to send the request to
     * @param header the request header
     * @param request the request body
     * @return a stage that completes with the response from the receiver
     * @throws IllegalArgumentException if the route name is not known to the enclosing router
     */
    CompletionStage<Response> sendRequest(
                                          String route,
                                          RequestHeaderData header,
                                          ApiMessage request);

    /**
     * Delivers a response to the client.
     *
     * @param response the response to send to the client
     */
    void sendResponse(Response response);

    /**
     * Disconnects the client.
     */
    void disconnect();

    /**
     * @return the unique identifier for the current proxy session
     */
    String sessionId();

    /**
     * @return the authenticated subject of the client, if authentication
     *         has been performed
     */
    Subject authenticatedSubject();
}
