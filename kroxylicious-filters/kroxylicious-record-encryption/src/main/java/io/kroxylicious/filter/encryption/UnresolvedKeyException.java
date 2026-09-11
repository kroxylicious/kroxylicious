/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption;

import io.kroxylicious.filter.encryption.common.EncryptionException;

/**
 * The encryption operation failed because we could not resolve a Key for some of the records.
 */
public class UnresolvedKeyException extends EncryptionException {
    /**
     * Creates an exception with the given message.
     * @param message the detail message.
     */
    public UnresolvedKeyException(String message) {
        super(message);
    }
}
