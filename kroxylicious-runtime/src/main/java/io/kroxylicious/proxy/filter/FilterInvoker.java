/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter;

import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;

/**
 * The FilterInvoker connects Kroxylicious with the concrete implementation of a Filter.
 * <p>When handling a message, we want to avoid the penalty of deserializing the bytes
 * into an ApiMessage. When Kroxylicious receives a message, all the Filters in the
 * filter chain will be consulted (via their invoker) to see if any want to handle that message.
 * If any filter wants to handle it, then the message will be deserialized. Then
 * onRequest|onResponse will be eligible to be called in the filter chain for that
 * message (i.e. if filter A wants to handle request Y but filter B doesn't, only the
 * onRequest of A would be invoked)</p>
 * <h2>Guarantees</h2>
 * <p>Implementors of this API may assume the following:</p>
 * <ol>
 *     <li>That each instance of the FilterInvoker is associated with a single channel</li>
 *     <li>That {@link #shouldHandleRequest(ApiKeys, short)} and
 *     {@link #onRequest(ApiKeys, short, RequestHeaderData, ApiMessage, FilterContext)} (or {@code on*Request} as appropriate)
 *     will always be invoked on the same thread.</li>
 *     <li>That filters are applied in the order they were configured.</li>
 * </ol>
 * <p>From 1. and 2. it follows that you can use member variables in your filter to
 * store channel-local state.</p>
 *
 * <p>Implementors should <strong>not</strong> assume:</p>
 * <ol>
 *     <li>That invokers in the same chain execute on the same thread. Thus inter-filter communication/state
 *     transfer needs to be thread-safe</li>
 * </ol>
 */
public interface FilterInvoker {

    /**
     * Should this Invoker handle a request with a given api key and api version.
     * Returning true implies that this request will be deserialized and
     * {@link #onRequest(ApiKeys, short, RequestHeaderData, ApiMessage, FilterContext)}
     * is eligible to be called with the deserialized data (if the message flows to that filter).
     *
     * @param apiKey the key of the message
     * @param apiVersion the version of the message
     * @return true if the message should be deserialized
     */
    default boolean shouldHandleRequest(ApiKeys apiKey, short apiVersion) {
        return false;
    }

    /**
     * Should this Invoker handle a response with a given api key and api version.
     * Returning true implies that this response will be deserialized and
     * {@link #onResponse(ApiKeys, short, ResponseHeaderData, ApiMessage, FilterContext)}
     * is eligible to be called with the deserialized data (if the message flows to that filter).
     *
     * @param apiKey the key of the message
     * @param apiVersion the version of the message
     * @return true if the message should be deserialized
     */
    default boolean shouldHandleResponse(ApiKeys apiKey, short apiVersion) {
        return false;
    }

    /**
     * <p>Handle deserialized request data. Implementations must tolerate being called with requests that this FilterInvoker
     * is NOT interested in handling. If any FilterInvoker in the chain should handle a response, then all invokers in the chain
     * are eligible to have their onRequest called. Implementations should forward requests they do not wish to operate on.
     * </p><p>
     * Filters must return a {@link CompletionStage<RequestFilterResult>} object.  This object
     * encapsulates the request to be forwarded and, optionally, orders for actions such as closing the connection or
     * dropping the request.
     * </p><p>
     * The {@link FilterContext} is the factory for {@link FilterResult} objects.  See
     * {@link FilterContext#forwardRequest(RequestHeaderData, ApiMessage)} and
     * {@link FilterContext#requestFilterResultBuilder()} for more details.
     * </p>
     * @param apiKey        the key of the message
     * @param apiVersion    the apiVersion of the message
     * @param header        the header of the message
     * @param body          the body of the message
     * @param filterContext contains methods to continue the filter chain and other contextual data
     * @return a {@link CompletionStage<RequestFilterResult>}, that when complete, will yield the request to forward.
     */
    default CompletionStage<RequestFilterResult> onRequest(
            ApiKeys apiKey,
            short apiVersion,
            RequestHeaderData header,
            ApiMessage body,
            FilterContext filterContext
    ) {
        return filterContext.requestFilterResultBuilder().forward(header, body).completed();
    }

    /**
     * <p>Handle deserialized response data. Implementations must tolerate being called with responses that this FilterInvoker
     * is NOT interested in handling. If any FilterInvoker in the chain should handle a response, then all invokers in the chain
     * are eligible to have their onResponse called. Implementations should forward responses they do not wish to operate on.
     * </p><p>
     * Filters must return a {@link CompletionStage<ResponseFilterResult>} object.  This object
     * encapsulates the response to be forwarded and, optionally, orders for actions such as closing the connection or
     * dropping the response.
     * </p><p>
     * The {@link FilterContext} is the factory for {@link FilterResult} objects.  See
     * {@link FilterContext#forwardResponse(ResponseHeaderData, ApiMessage)} and
     * {@link FilterContext#responseFilterResultBuilder()} for more details.
     * </p>
     *
     * @param apiKey        the key of the message
     * @param apiVersion    the apiVersion of the message
     * @param header        the header of the message
     * @param body          the body of the message
     * @param filterContext contains methods to continue the filter chain and other contextual data
     * @return a {@link CompletionStage<ResponseFilterResult>}, that when complete, will yield the response to forward.
     */
    default CompletionStage<ResponseFilterResult> onResponse(
            ApiKeys apiKey,
            short apiVersion,
            ResponseHeaderData header,
            ApiMessage body,
            FilterContext filterContext
    ) {
        return filterContext.responseFilterResultBuilder().forward(header, body).completed();
    }

}
