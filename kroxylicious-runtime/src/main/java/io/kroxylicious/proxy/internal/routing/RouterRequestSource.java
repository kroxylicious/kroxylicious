/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.routing;

import io.kroxylicious.proxy.bootstrap.RouterChainFactory;
import io.kroxylicious.proxy.frame.RequestFrame;

/**
 * Indicates that the enclosing {@link RoutingHandler} is nested — it intercepts
 * requests that a parent router dispatched to {@code activationRoute}, and ignores
 * all other frames (passing them through to the next handler).
 */
record RouterRequestSource(
                           String activationRoute,
                           RouterChainFactory routerChainFactory,
                           String routerName)
        implements RequestSource {

    /**
     * Returns {@code true} if this handler should intercept {@code frame} — i.e.
     * the frame's route name matches the activation route of this nested handler.
     */
    boolean intercepts(RequestFrame frame) {
        return activationRoute.equals(frame.routeName());
    }
}
