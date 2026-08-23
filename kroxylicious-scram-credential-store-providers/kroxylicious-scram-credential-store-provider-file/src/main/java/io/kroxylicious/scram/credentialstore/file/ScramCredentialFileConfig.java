/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.scram.credentialstore.file;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.kroxylicious.proxy.config.secret.PasswordProvider;

/**
 * Configuration for a proxy SCRAM credential file (backed by a Java KeyStore).
 *
 * @param file path to the proxy SCRAM credential file
 * @param filePassword password provider for the SCRAM credential file (also used to protect individual KeyStore entries)
 */
public record ScramCredentialFileConfig(
                                        @JsonProperty(required = true) String file,
                                        @JsonProperty(required = true) PasswordProvider filePassword) {

    /**
     * Canonical constructor with validation.
     */
    public ScramCredentialFileConfig {
        if (file == null) {
            throw new IllegalArgumentException("file must not be null");
        }
        if (file.isEmpty()) {
            throw new IllegalArgumentException("file must not be empty");
        }
        if (filePassword == null) {
            throw new IllegalArgumentException("filePassword must not be null");
        }
    }
}
