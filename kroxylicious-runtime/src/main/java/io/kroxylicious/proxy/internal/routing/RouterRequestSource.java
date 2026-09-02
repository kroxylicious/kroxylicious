/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.routing;

import io.kroxylicious.proxy.bootstrap.RouterChainFactory;
import io.kroxylicious.proxy.frame.PathElement;
import io.kroxylicious.proxy.frame.RequestFrame;

/**
 * Indicates that the enclosing {@link RoutingHandler} is nested — it intercepts
 * requests that a parent router dispatched to {@code activationPath}, and ignores
 * all other frames (passing them through to the next handler).
 */
record RouterRequestSource(
                           PathElement.Route activationPath,
                           RouterChainFactory routerChainFactory,
                           String routerName)
        implements RequestSource {

    /**
     * Returns {@code true} if this handler should intercept {@code frame} — i.e. the frame's
     * route position is exactly this nested handler's activation path. Both a router-issued
     * ({@code RouterContext.sendRequest}) and a filter-issued ({@code FilterContext.sendRequest})
     * out-of-band frame carry its own position ({@link PathElement.RouterOrigin} or {@link
     * PathElement.FilterOrigin} respectively) on top of the route position the request is
     * actually addressed to — {@link PathElement#routePosition()} strips it here before
     * comparing, since that position names the issuing router/filter's own promise, not a
     * position in the routing tree. A filter whose own route targets this nested router must be
     * intercepted here, exactly like ordinary traffic on that route, or its out-of-band request
     * could never reach this router's own (static or dynamic) routing decision.
     */
    boolean intercepts(RequestFrame frame) {
        PathElement path = frame.path();
        PathElement.RoutePosition routePosition = path == null ? null : path.routePosition();
        return activationPath.equals(routePosition);
    }
}
