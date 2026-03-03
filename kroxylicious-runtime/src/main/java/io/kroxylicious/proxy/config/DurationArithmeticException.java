/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config;

/**
 * Thrown if there is some arithmetic exception constructing or operating on a {@link java.time.Duration}
 */
public class DurationArithmeticException extends RuntimeException {

    public DurationArithmeticException(String message, ArithmeticException cause) {
        super(message, cause);
    }
}
