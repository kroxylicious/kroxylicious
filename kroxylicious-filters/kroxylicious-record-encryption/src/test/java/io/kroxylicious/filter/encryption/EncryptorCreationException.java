/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption;

public class EncryptorCreationException extends RuntimeException {
    public EncryptorCreationException(String message) {
        super(message);
    }
}
