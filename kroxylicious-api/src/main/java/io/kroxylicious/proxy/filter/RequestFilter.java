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
 * <p>A Filter that handles all request types, for example to modify the request headers.</p>
 *
 * <p>When a Filter implements {@code RequestFilter}:</p>
 * <ul>
 *  <li>it must not also implement any of the message-specific {@code *RequestFilter} interfaces like {@link ApiVersionsRequestFilter}.</li>
 *  <li>it may also implement either {@link ResponseFilter} or any of the message-specific {@code *ResponseFilter} interfaces, but not both.</li>
 * </ul>
 */
public interface RequestFilter extends Filter {

    /**
     * Does this filter implementation want to handle a request. If so, the
     * {@link #onRequest(ApiKeys, short, RequestHeaderData, ApiMessage, FilterContext)} method
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
     * @deprecated implement {@link #onRequest(ApiKeys, short, RequestHeaderData, ApiMessage, FilterContext)} instead.
     */
    @Deprecated(forRemoval = true, since = "0.19.0")
    default CompletionStage<RequestFilterResult> onRequest(ApiKeys apiKey,
                                                           RequestHeaderData header,
                                                           ApiMessage request,
                                                           FilterContext context) {
        throw new UnsupportedOperationException("implement #onRequest(ApiKeys, short, RequestHeaderData, ApiMessage, FilterContext)");
    }

    /**
     * Handle the given {@code header} and {@code request} pair, returning the {@code header} and {@code request}
     * pair to be passed to the next filter using the RequestFilterResult.
     * <br/>
     * The implementation may modify the given {@code header} and {@code request} in-place, or instantiate a
     * new instances.
     *
     * @param apiKey key of the request
     * @param apiVersion api version of the request
     * @param header header of the request
     * @param request body of the request
     * @param context context containing methods to continue the filter chain and other contextual data
     * @return a non-null CompletionStage that, when complete, will yield a RequestFilterResult containing the
     *         request to be forwarded.
     * @see io.kroxylicious.proxy.filter Creating Filter Result objects
     * @see io.kroxylicious.proxy.filter Thread Safety
     */
    @SuppressWarnings("deprecated")
    default CompletionStage<RequestFilterResult> onRequest(ApiKeys apiKey,
                                                           short apiVersion,
                                                           RequestHeaderData header,
                                                           ApiMessage request,
                                                           FilterContext context) {
        // default implementation exists so that pre-0.19.0 implementations of RequestFilter continue to work without change.
        // when the deprecated method is removed, remove this default implementation.
        return onRequest(apiKey, header, request, context);
    }

}
