/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.router;

/**
 * Marker for the completion of a router decision. The router uses
 * {@link RoutingContext#sendResponse(Response)} to deliver the response
 * to the client; this type simply signals that the router has finished
 * processing.
 */
public interface RoutingResult {

    /**
     * @return a result indicating that router completed normally
     */
    static RoutingResult completed() {
        return CompletedRoutingResult.INSTANCE;
    }
}
