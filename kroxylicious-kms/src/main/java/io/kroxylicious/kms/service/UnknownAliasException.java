/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.service;

/**
 * Thrown when a client tries to resolve a key with an alias which is not know to the KMS.
 */
public class UnknownAliasException extends KmsException {

    public UnknownAliasException(String alias) {
        super(alias);
    }
}
