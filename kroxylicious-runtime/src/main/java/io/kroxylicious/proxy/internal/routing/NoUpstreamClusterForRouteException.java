/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.routing;

// Exception thrown when the runtime tries to determine the upstream cluster for a route
// and one cannot be determined.
public class NoUpstreamClusterForRouteException extends RuntimeException {
    public NoUpstreamClusterForRouteException(String message) {
        super(message);
    }
}
