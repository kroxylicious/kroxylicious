/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.hashicorp.vault.config;

import java.net.URI;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonProperty.Access;

import io.kroxylicious.proxy.config.secret.PasswordProvider;
import io.kroxylicious.proxy.config.tls.Tls;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Configuration for the Vault KMS service.
 *
 * <p>Use the {@code credentials} node to specify the authentication method:
 * <ul>
 *   <li>{@code credentials.token} — static Vault token (replaces the deprecated top-level {@code vaultToken}).</li>
 *   <li>{@code credentials.kubernetes} — Kubernetes ServiceAccount JWT token auth.</li>
 * </ul>
 *
 * @param vaultTransitEngineUrl URL of the Vault Transit Engine e.g. {@code https://myhashicorpvault:8200/v1/transit}
 * @param vaultToken            the password provider that will provide the Vault token. Deprecated: use {@code credentials.token} instead.
 * @param credentials           grouped credential provider configuration.
 * @param tls                   TLS configuration used when connecting to Vault, or {@code null} if platform defaults are to be used.
 */
public record Config(
                     @JsonProperty(value = "vaultTransitEngineUrl", required = true) URI vaultTransitEngineUrl,
                     @Deprecated(since = "0.25.0", forRemoval = true) @JsonProperty(value = "vaultToken", required = false, access = Access.WRITE_ONLY) @Nullable PasswordProvider vaultToken,
                     @JsonProperty(value = "credentials", required = false) @Nullable VaultCredentialsConfig credentials,
                     @JsonProperty(value = "tls", required = false) @Nullable Tls tls) {

    /**
     * Validates and normalizes the configuration components.
     */
    public Config {
        Objects.requireNonNull(vaultTransitEngineUrl);
        if (vaultToken != null && credentials != null) {
            throw new IllegalArgumentException("Cannot specify both 'vaultToken' and 'credentials' - use 'credentials.token' instead");
        }
        if (vaultToken == null && credentials == null) {
            throw new IllegalArgumentException("Either 'credentials' or deprecated 'vaultToken' must be provided");
        }
        if (credentials == null) {
            credentials = new VaultCredentialsConfig(new TokenCredentialsConfig(vaultToken), null);
        }
    }

    /**
     * Convenience constructor for configuration using {@link VaultCredentialsConfig}.
     *
     * @param vaultTransitEngineUrl URL of the Vault Transit Engine.
     * @param credentials           grouped credential provider configuration.
     * @param tls                   TLS configuration.
     */
    public Config(URI vaultTransitEngineUrl, VaultCredentialsConfig credentials, @Nullable Tls tls) {
        this(vaultTransitEngineUrl, null, credentials, tls);
    }

    /**
     * Convenience constructor for configuration using deprecated {@link PasswordProvider}.
     *
     * @param vaultTransitEngineUrl URL of the Vault Transit Engine.
     * @param vaultToken            the password provider that will provide the Vault token.
     * @param tls                   TLS configuration.
     */
    @Deprecated(since = "0.25.0", forRemoval = true)
    public Config(URI vaultTransitEngineUrl, PasswordProvider vaultToken, @Nullable Tls tls) {
        this(vaultTransitEngineUrl, vaultToken, null, tls);
    }
}
