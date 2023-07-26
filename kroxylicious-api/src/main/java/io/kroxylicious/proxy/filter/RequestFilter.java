/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter;

import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;

/**
 * A KrpcFilter implementation intended to simplify cases where we want to handle all or
 * most request types, for example to modify the request headers. If a Filter implements
 * RequestFilter, it cannot also implement {@link CompositeFilter} or any of the specific
 * message Filter interfaces like {@link ApiVersionsRequestFilter}. If a Filter implements
 * RequestFilter, it may also implement {@link ResponseFilter}.
 */
public interface RequestFilter extends KrpcFilter {

    /**
     * Does this filter implementation want to handle a request. If so, the
     * {@link #onRequest(ApiKeys, RequestHeaderData, ApiMessage, KrpcFilterContext)} method
     * will be eligible to be called with the deserialized request data (if the
     * message reaches this filter in the filter chain).
     *
     * @param apiKeys the api key of the message
     * @param apiVersion the api version of the message
     * @return true if this filter wants to handle the request
     */
    default boolean shouldHandleRequest(ApiKeys apiKeys, short apiVersion) {
        return true;
    }

    /**
     * Handle the request
     *
     * @param apiKey        key of the request
     * @param header        header of the request
     * @param body          body of the request
     * @param filterContext context containing methods to continue the filter chain and other contextual data
     */
    CompletionStage<? extends FilterResult> onRequest(ApiKeys apiKey,
                                                      RequestHeaderData header,
                                                      ApiMessage body,
                                                      KrpcFilterContext filterContext);
}
