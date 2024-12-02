/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.encrypt;

import org.apache.kafka.common.errors.ApiException;

import io.kroxylicious.filter.encryption.common.EncryptionException;

/**
 * Request could not be satisfied. Indicates that there was some logical reason
 * an encryption/decryption request could not be satisfied. For example the
 * backing Key Management System is responding as we expect, generating DEKs for
 * us, but we are unable to obtain an Encryptor with capacity to encrypt all the
 * records in a batch for some reason.
 */
public class RequestNotSatisfiable extends EncryptionException {
    public RequestNotSatisfiable(String message) {
        super(message);
    }

    public RequestNotSatisfiable(String message, ApiException apiException) {
        super(message, apiException);
    }
}
