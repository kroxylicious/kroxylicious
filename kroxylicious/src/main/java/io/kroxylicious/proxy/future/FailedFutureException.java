/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.future;

/**
 * Exception that indicates an attempt to get the value of a future that failed.
 */
public class FailedFutureException extends RuntimeException {
    public FailedFutureException(Throwable cause) {
        super(cause);
    }
}
