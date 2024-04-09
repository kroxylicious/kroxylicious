/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.common;

/**
 * Exceptions to do with encryption.
 */
public class EncryptionException extends RuntimeException {
    public EncryptionException(Throwable e) {
        super(e);
    }

    public EncryptionException(String message) {
        super(message);
    }

}
