/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.doxylicious.exec;

public class ExecException extends Exception {
    public ExecException(String message) {
        super(message);
    }

    public ExecException(Throwable cause) {
        super(cause);
    }

    public ExecException(
                         String message,
                         Throwable cause) {
        super(message, cause);
    }
}
