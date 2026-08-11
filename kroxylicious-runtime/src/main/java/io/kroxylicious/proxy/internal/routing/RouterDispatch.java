/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.routing;

import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.ApiMessage;

/**
 * Abstraction over the dispatch capabilities that {@link RouterContextImpl} needs.
 * {@link RouteDispatcher} implements this, parameterised by route prefix for
 * both top-level and nested routing.
 */
interface RouterDispatch {

    Map<String, RouteDescriptor> routes();

    NodeIdMapping nodeIdMapping();

    CompletionStage<ApiMessage> sendToAnyNode(String route,
                                              RequestHeaderData header,
                                              ApiMessage request,
                                              String sessionId,
                                              int clientCorrelationId);

    CompletionStage<ApiMessage> sendToSpecificNode(int targetNodeId,
                                                   String route,
                                                   RequestHeaderData header,
                                                   ApiMessage request,
                                                   String sessionId,
                                                   int clientCorrelationId);
}
