/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.future;

/**
 * Exception that indicates an attempt to get the value or cause of a future that
 * is not yet done.
 */
public class UncompletedFutureException extends RuntimeException {
    public UncompletedFutureException() {
        super();
    }
}
