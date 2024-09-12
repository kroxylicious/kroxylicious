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
 * A Filter implementation intended to simplify cases where we want to handle all or
 * most request types, for example to modify the request headers. If a Filter implements
 * RequestFilter, it cannot also implement any of the specific message Filter interfaces
 * like {@link ApiVersionsRequestFilter}. If a Filter implements RequestFilter, it may
 * also implement {@link ResponseFilter}.
 */
public interface RequestFilter extends Filter {

    /**
     * Does this filter implementation want to handle a request. If so, the
     * {@link #onRequest(ApiKeys, RequestHeaderData, ApiMessage, FilterContext)} method
     * will be eligible to be called with the deserialized request data (if the
     * message reaches this filter in the filter chain).
     *
     * @param apiKey the api key of the message
     * @param apiVersion the api version of the message
     * @return true if this filter wants to handle the request
     */
    default boolean shouldHandleRequest(ApiKeys apiKey, short apiVersion) {
        return true;
    }

    /**
     * Handle the given {@code header} and {@code request} pair, returning the {@code header} and {@code request}
     * pair to be passed to the next filter using the RequestFilterResult.
     * <br/>
     * The implementation may modify the given {@code header} and {@code request} in-place, or instantiate a
     * new instances.
     *
     * @param apiKey  key of the request
     * @param header  header of the request
     * @param request body of the request
     * @param context context containing methods to continue the filter chain and other contextual data
     * @return a non-null CompletionStage that, when complete, will yield a RequestFilterResult containing the
     *         request to be forwarded.
     * @see io.kroxylicious.proxy.filter Creating Filter Result objects
     * @see io.kroxylicious.proxy.filter Thread Safety
     */
    CompletionStage<RequestFilterResult> onRequest(
            ApiKeys apiKey,
            RequestHeaderData header,
            ApiMessage request,
            FilterContext context
    );
}
