/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.invoker;

import org.apache.kafka.common.protocol.ApiKeys;

import io.kroxylicious.proxy.filter.KrpcFilter;
import io.kroxylicious.proxy.filter.KrpcFilterContext;
import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.frame.DecodedResponseFrame;

public interface FilterInvoker {
    /**
     * Apply the filter to the given {@code decodedFrame} using the given {@code filterContext}.
     * <p>Note that overriding this method on a Per-Message Filter implementation will do nothing
     * since we have implemented a separate invoker to call the Per-Message on* methods</p>
     *
     * @param decodedFrame  The request frame.
     * @param filterContext The filter context.
     */
    default void onRequest(DecodedRequestFrame<?> decodedFrame, KrpcFilterContext filterContext) {
        filterContext.forwardRequest(decodedFrame.body());
    }

    /**
     * Apply the filter to the given {@code decodedFrame} using the given {@code filterContext}.
     * <p>Note that overriding this method on a Per-Message Filter implementation will do nothing
     * since we have implemented a separate invoker to call the Per-Message on* methods</p>
     *
     * @param decodedFrame  The response frame.
     * @param filterContext The filter context.
     */
    default void onResponse(DecodedResponseFrame<?> decodedFrame, KrpcFilterContext filterContext) {
        filterContext.forwardResponse(decodedFrame.body());
    }

    /**
     * <p>Determines whether a request with the given {@code apiKey} and {@code apiVersion} should be deserialized.
     *
     * @param apiKey     The API key
     * @param apiVersion The API version
     */
    default boolean shouldDeserializeRequest(ApiKeys apiKey, short apiVersion) {
        return true;
    }

    /**
     * <p>Determines whether a response with the given {@code apiKey} and {@code apiVersion} should be deserialized.
     *
     * @param apiKey     The API key
     * @param apiVersion The API version
     */
    default boolean shouldDeserializeResponse(ApiKeys apiKey, short apiVersion) {
        return true;
    }

    KrpcFilter getFilter();
}
