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
 * <p>
 * Supports two authentication methods (mutually exclusive):
 * <ul>
 *   <li>User authentication: requires {@code userCredentials}</li>
 *   <li>Client certificate authentication: requires {@code clientCredentials} and {@code tls.key}</li>
 * </ul>
 *
 * @param endpointUrl URL of the CipherTrust Manager instance e.g. {@code https://ctm.example.com}
 * @param userCredentials user credentials for username/password authentication (optional)
 * @param clientCredentials client credentials for client certificate authentication (optional)
 * @param tls TLS configuration including optional client certificate
 */
public record Config(
                     @JsonProperty(value = "endpointUrl", required = true) URI endpointUrl,
                     @JsonProperty(value = "userCredentials", required = false) @Nullable UserCredentials userCredentials,
                     @JsonProperty(value = "clientCredentials", required = false) @Nullable ClientCredentials clientCredentials,
                     @JsonProperty(value = "tls", required = false) @Nullable Tls tls) {

    /**
     * Constructs a CipherTrust Manager KMS configuration.
     * <p>
     * Validates that exactly one authentication method is configured:
     * either userCredentials OR clientCredentials (with client certificate in TLS).
     * </p>
     */
    public Config {
        Objects.requireNonNull(endpointUrl, "endpointUrl cannot be null");

        boolean hasUserCredentials = userCredentials != null;
        boolean hasClientCredentials = clientCredentials != null;
        boolean hasClientCertificate = tls != null && tls.key() != null;

        // Validate mutual exclusivity of authentication methods
        if (hasUserCredentials && hasClientCredentials) {
            throw new IllegalArgumentException("Cannot configure both userCredentials and clientCredentials");
        }

        // Validate that at least one authentication method is configured
        if (!hasUserCredentials && !hasClientCredentials) {
            throw new IllegalArgumentException("Must configure either userCredentials or clientCredentials");
        }

        // Validate client certificate authentication configuration
        if (hasClientCredentials && !hasClientCertificate) {
            throw new IllegalArgumentException("clientCredentials requires a client certificate (tls.key must be configured)");
        }
    }
}
