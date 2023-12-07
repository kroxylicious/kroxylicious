/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption;

/**
 * Represents a misconfiguration of encryption.
 * Exceptions of this type mean the proxy admin has messed up.
 */
public class EncryptionConfigurationException extends EncryptionException {

    public EncryptionConfigurationException(String message) {
        super(message);
    }
}
