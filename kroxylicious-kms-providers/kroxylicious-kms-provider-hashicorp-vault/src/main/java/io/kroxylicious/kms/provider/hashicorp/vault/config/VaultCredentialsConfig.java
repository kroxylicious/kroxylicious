/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.hashicorp.vault.config;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Groups HashiCorp Vault credential provider configuration under a single {@code credentials} node.
 *
 * <p>Example YAML (static token):
 * <pre>{@code
 * credentials:
 *   vaultToken:
 *     token:
 *       password: s.myVaultToken
 * }</pre>
 *
 * @param vaultToken static Vault token credentials.
 */
public record VaultCredentialsConfig(
                                     @JsonProperty("vaultToken") TokenCredentialsConfig vaultToken) {

    /**
     * Validates that vaultToken is provided.
     */
    public VaultCredentialsConfig {
        Objects.requireNonNull(vaultToken, "vaultToken credentials must be provided");
    }
}
