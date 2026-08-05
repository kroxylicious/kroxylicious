/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.model.networking;

/**
 * Thrown when the networking planner encounters an invalid or unsupported ingress configuration.
 */
public class NetworkPlanningException extends RuntimeException {

    /**
     * Creates a new NetworkPlanningException.
     * @param message the detail message
     */
    public NetworkPlanningException(String message) {
        super(message);
    }

}
