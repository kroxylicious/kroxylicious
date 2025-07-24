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
 * <p>A Filter that handles all response types.</p>
 *
 * <p>When a Filter implements {@code ResponseFilter}:</p>
 * <ul>
 *  <li>it must not also implement any of the message-specific {@code *ResponseFilter} interfaces like {@link ApiVersionsResponseFilter}.</li>
 *  <li>it may also implement either {@link RequestFilter} or any of the message-specific {@code *RequestFilter} interfaces, but not both.</li>
 * </ul>
 */
public interface ResponseFilter extends Filter {

    /**
     * Does this filter implementation want to handle a response. If so, the
     * {@link #onResponse(ApiKeys, ResponseHeaderData, ApiMessage, FilterContext)} method
     * will be eligible to be called with the deserialized response data (if the
     * message reaches this filter in the filter chain).
     *
     * @param apiKey     the api key of the message
     * @param apiVersion the api version of the message
     * @return true if this filter wants to handle the response
     */
    default boolean shouldHandleResponse(ApiKeys apiKey, short apiVersion) {
        return true;
    }

    /**
     * Handle the given {@code header} and {@code response} pair, returning the {@code header} and {@code response}
     * pair to be passed to the next filter using the ResponseFilterResult.
     * <br/>
     * The implementation may modify the given {@code header} and {@code response} in-place, or instantiate a
     * new instances.
     *
     * @param apiKey   key of the request
     * @param header   response header.
     * @param response The body to handle.
     * @param context  The context.
     * @return a non-null CompletionStage that, when complete, will yield a ResponseFilterResult containing the
     *         response to be forwarded.
     * @see io.kroxylicious.proxy.filter Creating Filter Result objects
     * @see io.kroxylicious.proxy.filter Thread Safety
     */
    CompletionStage<ResponseFilterResult> onResponse(ApiKeys apiKey,
                                                     ResponseHeaderData header,
                                                     ApiMessage response,
                                                     FilterContext context);
}
