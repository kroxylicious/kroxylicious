/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.invoker;

import io.kroxylicious.proxy.filter.KrpcFilterContext;
import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.frame.DecodedResponseFrame;
import io.kroxylicious.proxy.internal.codec.DecodePredicate;

public interface FilterInvoker extends DecodePredicate {
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
}
