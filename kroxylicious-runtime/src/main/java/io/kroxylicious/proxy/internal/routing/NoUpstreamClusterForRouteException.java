/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.routing;

/**
 * Thrown when the runtime tries to determine the upstream cluster for a route
 * and one cannot be determined.
 */
public class NoUpstreamClusterForRouteException extends RuntimeException {
    /**
     * Creates the exception.
     *
     * @param message detail message identifying the route without an upstream cluster
     */
    public NoUpstreamClusterForRouteException(String message) {
        super(message);
    }
}
