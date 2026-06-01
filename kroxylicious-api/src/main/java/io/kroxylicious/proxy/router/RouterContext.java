/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.router;

import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.ApiMessage;

import io.kroxylicious.proxy.authentication.Subject;

/**
 * Context passed to {@link Router#onRequest} for issuing requests
 * to named routes.
 */
public interface RouterContext {

    /**
     * Returns the virtual node ID of a broker on the named route's cluster.
     *
     * <p>This is used to send initial requests (e.g. {@code METADATA}) before
     * the router has discovered the cluster's full broker topology. Once
     * {@code METADATA} responses arrive, the router uses the virtual node IDs
     * from those responses to address specific brokers.</p>
     *
     * <p>The runtime selects which broker to return.</p>
     *
     * @param route the name of the route
     * @return the virtual node ID of a bootstrap broker on the route's cluster
     * @throws IllegalArgumentException if the route name is not known
     */
    int bootstrapNodeId(String route);

    /**
     * Sends a request to a specific broker identified by route and virtual node ID.
     *
     * <p>The runtime uses the route to determine which target cluster, and
     * resolves the virtual node ID to a specific upstream broker address,
     * opening a new connection if necessary. The returned stage completes
     * when the broker produces a response.</p>
     *
     * @param route the name of the route
     * @param virtualNodeId the virtual node ID of the target broker
     * @param header the request header
     * @param request the request body
     * @return a stage that completes with the response from the broker
     * @throws IllegalArgumentException if the route name is not known
     * @throws IllegalStateException if the upstream address for the node is
     *         not yet known (metadata not yet reconciled)
     */
    CompletionStage<Response> sendRequestToNode(
                                                String route,
                                                int virtualNodeId,
                                                RequestHeaderData header,
                                                ApiMessage request);

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
