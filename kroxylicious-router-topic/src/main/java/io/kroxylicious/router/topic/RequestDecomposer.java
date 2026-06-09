/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.router.topic;

import java.util.Map;

import org.apache.kafka.common.protocol.ApiMessage;

/**
 * Decomposes a batched Kafka request into per-route sub-requests
 * and recomposes the corresponding sub-responses into a single
 * response for the client.
 *
 * @param <Req> the request message type
 * @param <Resp> the response message type
 */
public interface RequestDecomposer<Req extends ApiMessage, Resp extends ApiMessage> {

    /**
     * Splits a single client request into per-route sub-requests.
     * The returned map is keyed by route name. If all topics in the
     * request belong to a single route, the map has one entry and
     * the implementation may return the original request unchanged.
     *
     * @param request the client's original request body
     * @param table the topic-to-route mapping
     * @return per-route sub-requests, never empty
     */
    Map<String, Req> decompose(Req request,
                               TopicRoutingTable table);

    /**
     * Merges per-route sub-responses into a single response for the client.
     *
     * @param responses per-route responses keyed by route name
     * @param originalRequest the original undivided client request
     * @return the merged response
     */
    Resp recompose(Map<String, Resp> responses,
                   Req originalRequest);
}
