/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption;

import io.kroxylicious.filter.encryption.common.EncryptionException;

/**
 * An encryption operation could not be completed due to an encryption policy. For example
 * if all topics are required to be encrypted, and we cannot resolve an encryption key for
 * topic X, then we cannot fulfil the policy and fail with this exception.
 */
public class EncryptionPolicyException extends EncryptionException {
    public EncryptionPolicyException(String message) {
        super(message);
    }
}
