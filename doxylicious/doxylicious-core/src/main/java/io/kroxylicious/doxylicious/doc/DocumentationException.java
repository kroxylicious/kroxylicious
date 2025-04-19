/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.doxylicious.doc;

public class DocumentationException extends RuntimeException {
    public DocumentationException(Throwable cause) {
        super(cause);
    }

    public DocumentationException(String message) {
        super(message);
    }

    public DocumentationException(
                                  String message,
                                  Throwable cause) {
        super(message, cause);
    }
}
