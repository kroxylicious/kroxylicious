/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.tools.schema;

public class InvalidSchemaException extends RuntimeException {
    public InvalidSchemaException(Throwable cause) {
        super(cause);
    }

    public InvalidSchemaException(String message) {
        super(message);
    }

    public InvalidSchemaException(
                                  String message,
                                  Throwable cause) {
        super(message, cause);
    }
}
