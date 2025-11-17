/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.authorizer.service;

/**
 * An exception to be thrown if an {@link Authorizer} cannot be built, or is not able to make a decision.
 */
public class AuthorizerException extends RuntimeException {
    public AuthorizerException(String message) {
        super(message);
    }
    public AuthorizerException(String message, Throwable cause) {
        super(message, cause);
    }
}
