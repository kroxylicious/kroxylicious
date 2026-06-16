/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.thales.ciphertrust.config;

import java.net.URI;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.kroxylicious.proxy.config.tls.Tls;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Configuration for the Thales CipherTrust Manager KMS service.
 *
 * @param endpointUrl URL of the CipherTrust Manager instance e.g. {@code https://ctm.example.com}
 * @param userCredentials user credentials for authentication
 * @param tls TLS configuration
 */
public record Config(
                     @JsonProperty(value = "endpointUrl", required = true) URI endpointUrl,
                     @JsonProperty(value = "userCredentials", required = true) UserCredentials userCredentials,
                     @JsonProperty(value = "tls", required = false) @Nullable Tls tls) {

    /**
     * Constructs a CipherTrust Manager KMS configuration.
     */
    public Config {
        Objects.requireNonNull(endpointUrl, "endpointUrl cannot be null");
        Objects.requireNonNull(userCredentials, "userCredentials cannot be null");
    }
}
