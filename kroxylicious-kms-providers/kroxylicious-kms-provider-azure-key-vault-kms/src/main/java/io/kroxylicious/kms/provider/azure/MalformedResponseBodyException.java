/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.azure;

public class MalformedResponseBodyException extends RuntimeException {
    public MalformedResponseBodyException(String message, Throwable cause) {
        super(message, cause);
    }

    public MalformedResponseBodyException(String message) {
        super(message);
    }
}
