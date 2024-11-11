/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

/**
 * An exception specific to a cluster within a proxy.
 */
public class InvalidClusterException extends RuntimeException {
    private final String reason;

    public InvalidClusterException(String clusterName, String reason, String message) {
        super("Cluster \"" + clusterName + "\": " + message);
        this.reason = reason;
    }

    public String reason() {
        return reason;
    }
}
