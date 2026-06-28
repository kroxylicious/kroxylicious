/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.dek;

public class DekException extends RuntimeException {
    public DekException(String message) {
        super(message);
    }

    public DekException() {
        super();
    }

    public DekException(Throwable cause) {
        super(cause);
    }
}
