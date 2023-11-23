/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.k8s.exception;

/**
 * The type Wait exception.
 */
public class WaitException extends RuntimeException {
    /**
     * Instantiates a new Wait exception.
     *
     * @param message the message
     */
    public WaitException(String message) {
        super(message);
    }

    /**
     * Instantiates a new Wait exception.
     *
     * @param cause the cause
     */
    public WaitException(Throwable cause) {
        super(cause);
    }
}
