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
 * The FilterInvoker connects Kroxylicious with the concrete implementation of a KrpcFilter.
 * <p>When handling a message, we want to avoid the penalty of deserializing the bytes
 * into an ApiMessage. When Kroxylicious receives a message, all the Filters in the
 * filter chainnwill be consulted (via their invoker) to see if any want to handle that message.
 * If any filter wants to handle it, then the message will be deserialized. Then
 * onRequest|onResponse will be eligible to be called in the filter chain for that
 * message (i.e. if filter A wants to handle request Y but filter B doesn't, only the
 * onRequest of A would be invoked)</p>
 * <h2>Guarantees</h2>
 * <p>Implementors of this API may assume the following:</p>
 * <ol>
 *     <li>That each instance of the FilterInvoker is associated with a single channel</li>
 *     <li>That {@link #shouldHandleRequest(ApiKeys, short)} and
 *     {@link #onRequest(ApiKeys, short, RequestHeaderData, ApiMessage, KrpcFilterContext)} (or {@code on*Request} as appropriate)
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
     * {@link #onRequest(ApiKeys, short, RequestHeaderData, ApiMessage, KrpcFilterContext)}
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
     * {@link #onResponse(ApiKeys, short, ResponseHeaderData, ApiMessage, KrpcFilterContext)}
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
     * <p>Handle deserialized request data. It is implicit that the underlying filter
     * wants to handle this data because it indicated that with {@link #shouldHandleRequest(ApiKeys, short)}
     * </p><p>
     * Most Filters will want to call a method on {@link KrpcFilterContext} like {@link KrpcFilterContext#completedForwardRequest(RequestHeaderData, ApiMessage)}
     * so that the message continues to flow through the filter chain.
     * </p>
     *
     * @param apiKey        the key of the message
     * @param apiVersion    the apiVersion of the message
     * @param header        the header of the message
     * @param body          the body of the message
     * @param filterContext contains methods to continue the filter chain and other contextual data
     * @return
     */
    default CompletionStage<? extends FilterResult> onRequest(ApiKeys apiKey, short apiVersion, RequestHeaderData header, ApiMessage body,
                                                              KrpcFilterContext filterContext) {
        return filterContext.completedForwardRequest(header, body);
    }

    /**
     * <p>Handle deserialized response data. It is implicit that the underlying filter
     * wants to handle this data because it indicated that with {@link #shouldHandleResponse(ApiKeys, short)}
     * </p><p>
     * Most Filters will want to call a method on {@link KrpcFilterContext} like {@link KrpcFilterContext#completedForwardResponse(ResponseHeaderData, ApiMessage)}
     * so that the message continues to flow through the filter chain.
     * </p>
     *
     * @param apiKey        the key of the message
     * @param apiVersion    the apiVersion of the message
     * @param header        the header of the message
     * @param body          the body of the message
     * @param filterContext contains methods to continue the filter chain and other contextual data
     * @return
     */
    default CompletionStage<ResponseFilterResult> onResponse(ApiKeys apiKey, short apiVersion, ResponseHeaderData header, ApiMessage body,
                                                             KrpcFilterContext filterContext) {
        return filterContext.completedForwardResponse(header, body);
    }

}
