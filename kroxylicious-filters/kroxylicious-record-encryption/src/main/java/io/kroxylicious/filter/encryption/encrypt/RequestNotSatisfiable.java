/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.encrypt;

import io.kroxylicious.filter.encryption.common.EncryptionException;
import io.kroxylicious.kafka.common.errors.ApiException;

/**
 * Request could not be satisfied. Indicates that there was some logical reason
 * an encryption/decryption request could not be satisfied. For example the
 * backing Key Management System is responding as we expect, generating DEKs for
 * us, but we are unable to obtain an Encryptor with capacity to encrypt all the
 * records in a batch for some reason.
 */
public class RequestNotSatisfiable extends EncryptionException {
    /**
     * Creates an exception with the given message.
     * @param message the detail message.
     */
    public RequestNotSatisfiable(String message) {
        super(message);
    }

    /**
     * Creates an exception with the given message and client-facing exception.
     * @param message the detail message.
     * @param apiException the exception to be sent to the client.
     */
    public RequestNotSatisfiable(String message, ApiException apiException) {
        super(message, apiException);
    }
}
