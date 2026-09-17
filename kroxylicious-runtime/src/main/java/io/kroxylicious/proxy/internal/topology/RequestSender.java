/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.topology;

import java.util.concurrent.CompletionStage;

import io.kroxylicious.kafka.common.message.RequestHeaderData;
import io.kroxylicious.kafka.common.protocol.ApiMessage;

/**
 * The minimal request-sending capability {@link io.kroxylicious.proxy.topology.TopologyService}
 * discovery methods need: sending an internal request to any broker on a route and awaiting its
 * response. Deliberately narrower than the full dispatch surface a {@code Router} gets via
 * {@code RouterContext} - every discovery method (topic name resolution, leader/coordinator
 * discovery) only ever needs "any node on this route".
 */
@FunctionalInterface
public interface RequestSender {

    /**
     * Sends a request to any available node on the given route and returns a stage that
     * completes with the decoded response body.
     *
     * @param route the route name identifying the target cluster
     * @param header the Kafka request header
     * @param request the decoded request body
     * @return a stage that completes with the decoded response body
     */
    CompletionStage<ApiMessage> sendToAnyNode(String route, RequestHeaderData header, ApiMessage request);

    /**
     * A sender that always fails. Used for the shared, {@code RouterFactory#initialize}-time
     * {@link io.kroxylicious.proxy.topology.TopologyService} instance, which
     * {@link io.kroxylicious.proxy.router.RouterFactoryContext#topologyService()} documents as an
     * instance that must not be stored or used for discovery.
     *
     * @return a sender that always throws
     */
    static RequestSender unavailable() {
        return (route, header, request) -> {
            throw new IllegalStateException(
                    "TopologyService discovery methods must not be invoked on the instance obtained during "
                            + "RouterFactory#initialize; store and use only the instance returned from RouterFactory#createRouter");
        };
    }
}
