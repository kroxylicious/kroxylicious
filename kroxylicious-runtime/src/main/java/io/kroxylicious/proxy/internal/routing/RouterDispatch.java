/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.routing;

import java.util.Map;
import java.util.concurrent.CompletionStage;

import io.kroxylicious.kafka.common.message.RequestHeaderData;
import io.kroxylicious.kafka.common.protocol.ApiMessage;

/**
 * Abstraction over the dispatch capabilities that {@link RouterContextImpl} needs.
 * {@link RouteDispatcher} implements this, parameterised by route prefix for
 * both top-level and nested routing.
 */
interface RouterDispatch {

    /**
     * Returns all route descriptors visible at this routing level, keyed by route name.
     * Top-level routes use unqualified names; nested routes use qualified names of the
     * form {@code routerName/routeName}.
     */
    Map<String, RouteDescriptor> routes();

    /**
     * Returns the {@link NodeIdMapping} for this routing level, used to translate
     * between target-cluster node IDs and the virtual node IDs seen by the client.
     */
    NodeIdMapping nodeIdMapping();

    /**
     * Sends a request to any available node on the target cluster for the given route,
     * letting the upstream broker select the appropriate node.
     *
     * @param route the route name identifying the target cluster
     * @param header the Kafka request header (must carry the client's correlation ID)
     * @param request the decoded request body
     * @param sessionId the proxy session ID, used for logging and diagnostics
     * @param clientCorrelationId the correlation ID from the client's request header,
     *        used to match the upstream response back to the pending future
     * @return a stage that completes with the decoded response body
     */
    CompletionStage<ApiMessage> sendToAnyNode(String route,
                                              RequestHeaderData header,
                                              ApiMessage request,
                                              String sessionId,
                                              int clientCorrelationId);

    /**
     * Sends a request to a specific virtual node ID on the target cluster for the given route.
     * The virtual node ID is translated to a target-cluster node ID via {@link NodeIdMapping#fromVirtual(String, int)}.
     *
     * @param targetNodeId the virtual node ID to send to
     * @param route the route name identifying the target cluster
     * @param header the Kafka request header (must carry the client's correlation ID)
     * @param request the decoded request body
     * @param sessionId the proxy session ID, used for logging and diagnostics
     * @param clientCorrelationId the correlation ID from the client's request header,
     *        used to match the upstream response back to the pending future
     * @return a stage that completes with the decoded response body
     */
    CompletionStage<ApiMessage> sendToSpecificNode(int targetNodeId,
                                                   String route,
                                                   RequestHeaderData header,
                                                   ApiMessage request,
                                                   String sessionId,
                                                   int clientCorrelationId);
}
