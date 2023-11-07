/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.service;

/**
 * Thrown when a KMS-managed key is passed to an operation that is incompatible with its allowed key usage.
 * E.g. when a key intended to be used for signing/verifying key, is passed used in an key wrapping operation.
 */
public class InvalidKeyUsageException extends KmsException {
    /**
     * Initializes a new instance.
     */
    public InvalidKeyUsageException() {
        super();
    }
}
