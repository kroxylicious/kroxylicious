/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.sasl.credentialstore.keystore;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.kroxylicious.proxy.config.secret.PasswordProvider;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Configuration for KeyStore-based SCRAM credential store.
 *
 * @param file path to the Java KeyStore file
 * @param storePassword password provider for the KeyStore
 * @param keyPassword optional password provider for individual keys (defaults to storePassword if not specified)
 * @param storeType KeyStore type (e.g., "PKCS12", "JKS"). Defaults to KeyStore.getDefaultType() if not specified.
 */
public record KeystoreScramCredentialStoreConfig(
                                                 @JsonProperty(required = true) String file,
                                                 @JsonProperty(required = true) PasswordProvider storePassword,
                                                 @Nullable PasswordProvider keyPassword,
                                                 @Nullable String storeType) {

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

    /**
     * Get the key password, defaulting to store password if not specified.
     *
     * @return the key password provider
     */
    public PasswordProvider effectiveKeyPassword() {
        return keyPassword != null ? keyPassword : storePassword;
    }

    /**
     * Get the store type, defaulting to platform default if not specified.
     *
     * @return the KeyStore type
     */
    public String effectiveStoreType() {
        return storeType != null ? storeType : java.security.KeyStore.getDefaultType();
    }
}
