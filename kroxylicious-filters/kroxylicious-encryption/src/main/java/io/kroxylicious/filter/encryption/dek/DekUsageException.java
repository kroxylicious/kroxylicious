/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.dek;

/**
 * Indicates an attempt to use a DEK in a way that isn't allowed, for example
 * to encrypt when its limit on number of encryption operations has been reached.
 */
public class DekUsageException extends RuntimeException {
    public DekUsageException(String message) {
        super(message);
    }
}
