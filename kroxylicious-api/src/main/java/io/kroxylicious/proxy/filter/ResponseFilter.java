/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter;

import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;

/**
 * A KrpcFilter implementation intended to simplify cases where we want to handle all or
 * most response types, for example to modify the response headers. If a Filter implements
 * ResponseFilter, it cannot also implement {@link CompositeFilter} or any of the specific
 * message Filter interfaces like {@link ApiVersionsRequestFilter}. If a Filter implements
 * ResponseFilter, it may also implement {@link RequestFilter}.
 */
public interface ResponseFilter extends KrpcFilter {

    /**
     * Does this filter implementation want to handle a response. If so, the
     * {@link #onResponse(ApiKeys, ResponseHeaderData, ApiMessage, KrpcFilterContext)} method
     * will be eligible to be called with the deserialized response data (if the
     * message reaches this filter in the filter chain).
     *
     * @param apiKeys the api key of the message
     * @param apiVersion the api version of the message
     * @return true if this filter wants to handle the response
     */
    default boolean shouldHandleResponse(ApiKeys apiKeys, short apiVersion) {
        return true;
    }

    /**
     * Handle the response
     * @param apiKey key of the response
     * @param header header of the response
     * @param body body of the response
     * @param filterContext context containing methods to continue the filter chain and other contextual data
     * @return CompletionStage that, when complete, will yield a ResponseFilterResult containing the
     *         response to be forwarded.
     */
    CompletionStage<ResponseFilterResult> onResponse(ApiKeys apiKey,
                                                     ResponseHeaderData header,
                                                     ApiMessage body,
                                                     KrpcFilterContext filterContext);
}
