/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.dek;

/**
 * Indicates an attempt to deserialize a persisted reference to a CipherSpec, but the persisted identifier was unknown.
 */
public class UnknownCipherSpecException extends DekException {
    public UnknownCipherSpecException(String message) {
        super(message);
    }
}
