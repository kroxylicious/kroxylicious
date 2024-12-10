/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.tools.schema.model;

public class VisitException extends RuntimeException {
    public VisitException(
                          String message,
                          Throwable cause) {
        super(message, cause);
    }
}
