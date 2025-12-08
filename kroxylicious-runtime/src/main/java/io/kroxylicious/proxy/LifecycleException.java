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
    public LifecycleException(String message, Throwable cause) {
        super(message, cause);
    }
}
