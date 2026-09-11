/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy;

/**
 * An exception thrown during a lifecycle transition such as startup or shutdown
 * of the proxy.
 */
public class LifecycleException extends RuntimeException {
    /**
     * Creates the exception with the given message and cause.
     *
     * @param message the detail message
     * @param cause the underlying cause of the lifecycle failure
     */
    public LifecycleException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Creates the exception with the given message.
     *
     * @param message the detail message
     */
    public LifecycleException(String message) {
        super(message);
    }
}
