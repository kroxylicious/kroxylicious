/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.thales.ciphertrust.config;

import java.net.URI;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.kroxylicious.kms.service.KmsException;
import io.kroxylicious.proxy.config.tls.Tls;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Configuration for the Thales CipherTrust Manager KMS service.
 *
 * @param endpointUrl URL of the CipherTrust Manager instance e.g. {@code https://ctm.example.com}
 * @param userCredentials user credentials for authentication (mutually exclusive with clientCredentials)
 * @param clientCredentials client credentials for authentication (mutually exclusive with userCredentials)
 * @param tls TLS configuration
 */
public record Config(
                     @JsonProperty(value = "endpointUrl", required = true) URI endpointUrl,
                     @JsonProperty("userCredentials") @Nullable UserCredentials userCredentials,
                     @JsonProperty("clientCredentials") @Nullable ClientCredentials clientCredentials,
                     @JsonProperty(value = "tls", required = false) @Nullable Tls tls) {

    /**
     * Constructs a CipherTrust Manager KMS configuration.
     */
    public Config {
        Objects.requireNonNull(endpointUrl, "endpointUrl cannot be null");

        // Exactly one credential type must be specified
        if (userCredentials == null && clientCredentials == null) {
            throw new KmsException("Either userCredentials or clientCredentials must be specified");
        }
        if (userCredentials != null && clientCredentials != null) {
            throw new KmsException("Cannot specify both userCredentials and clientCredentials");
        }
    }
}
