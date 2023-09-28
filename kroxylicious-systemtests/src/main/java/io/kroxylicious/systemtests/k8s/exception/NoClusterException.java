/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.k8s.exception;

/**
 * The type No cluster exception.
 */
public class NoClusterException extends RuntimeException {
    /**
     * Instantiates a new No cluster exception.
     *
     * @param message the message
     */
    public NoClusterException(String message) {
        super(message);
    }
}
