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
    private final ClusterCondition accepted;

    public InvalidClusterException(ClusterCondition accepted) {
        super();
        this.accepted = accepted;
    }

    public ClusterCondition accepted() {
        return accepted;
    }
}
