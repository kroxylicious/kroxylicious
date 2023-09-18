/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.k8s.exception;

public class WaitException extends RuntimeException {
    public WaitException(String message) {
        super(message);
    }

    public WaitException(Throwable cause) {
        super(cause);
    }
}
