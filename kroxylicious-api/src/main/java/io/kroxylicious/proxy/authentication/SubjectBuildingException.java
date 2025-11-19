/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.authentication;

/**
 * An exception to be thrown if a {@link Subject} cannot be built.
 */
public class SubjectBuildingException extends RuntimeException {
    public SubjectBuildingException(String message) {
        super(message);
    }

    public SubjectBuildingException(String message, Throwable cause) {
        super(message, cause);
    }
}
