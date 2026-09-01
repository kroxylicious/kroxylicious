/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.hashicorp.vault.config;

import java.net.URI;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.kroxylicious.proxy.config.secret.PasswordProvider;
import io.kroxylicious.proxy.config.tls.Tls;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Configuration for the Vault KMS service.
 * @param vaultTransitEngineUrl URL of the Vault Transit Engine e.g. {@code https://myhashicorpvault:8200/v1/transit}
 * @param vaultToken the password provider that will provide the Vault token.
 * @param role the Vault role bound to the ServiceAccount (required for Kubernetes auth)
 * @param serviceAccountTokenPath the path to the Kubernetes ServiceAccount token
 * @param authPath the mount path for the Kubernetes auth method in Vault
 * @param tls TLS configuration used when connecting to Vault, or {@code null} if platform defaults are to be used.
 */
public record Config(
                     @JsonProperty(value = "vaultTransitEngineUrl", required = true) URI vaultTransitEngineUrl,
                     @JsonProperty(value = "vaultToken", required = false) @Nullable PasswordProvider vaultToken,
                     @JsonProperty(value = "role", required = false) @Nullable String role,
                     @JsonProperty(value = "serviceAccountTokenPath", required = false) @Nullable String serviceAccountTokenPath,
                     @JsonProperty(value = "authPath", required = false) @Nullable String authPath,
                     @Nullable Tls tls) {
    /**
     * Validates the record components.
     */
    public Config {
        Objects.requireNonNull(vaultTransitEngineUrl);
        if (vaultToken == null && role == null) {
            throw new IllegalArgumentException("Either vaultToken or role must be provided");
        }
        if (vaultToken != null && role != null) {
            throw new IllegalArgumentException("Only one of vaultToken or role may be provided");
        }
        if (serviceAccountTokenPath == null) {
            serviceAccountTokenPath = "/var/run/secrets/kubernetes.io/serviceaccount/token";
        }
        if (authPath == null) {
            authPath = "kubernetes";
        }
    }

}
