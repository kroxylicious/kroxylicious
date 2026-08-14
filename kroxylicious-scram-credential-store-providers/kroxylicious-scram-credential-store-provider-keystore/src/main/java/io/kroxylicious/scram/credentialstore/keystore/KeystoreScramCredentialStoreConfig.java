/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.scram.credentialstore.keystore;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.kroxylicious.proxy.config.secret.PasswordProvider;

/**
 * Configuration for KeyStore-based SCRAM credential store.
 *
 * @param file path to the Java KeyStore file
 * @param storePassword password provider for the KeyStore (also used for individual key entries)
 */
public record KeystoreScramCredentialStoreConfig(
                                                 @JsonProperty(required = true) String file,
                                                 @JsonProperty(required = true) PasswordProvider storePassword) {

    /**
     * Canonical constructor with validation.
     */
    public KeystoreScramCredentialStoreConfig {
        if (file == null) {
            throw new IllegalArgumentException("file must not be null");
        }
        if (file.isEmpty()) {
            throw new IllegalArgumentException("file must not be empty");
        }
        if (storePassword == null) {
            throw new IllegalArgumentException("storePassword must not be null");
        }
    }
}
