/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.hashicorp.vault.config;

import com.fasterxml.jackson.annotation.JsonProperty;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Groups all HashiCorp Vault credential provider configurations under a single {@code credentials} node.
 * Exactly one field must be non-{@code null}.
 *
 * <p>Example YAML (static token):
 * <pre>{@code
 * credentials:
 *   vaultToken:
 *     token:
 *       password: s.myVaultToken
 * }</pre>
 *
 * <p>Example YAML (Kubernetes auth):
 * <pre>{@code
 * credentials:
 *   kubernetes:
 *     vaultRole: kroxylicious-vault-role
 * }</pre>
 *
 * @param vaultToken static Vault token credentials; mutually exclusive with {@code kubernetes}.
 * @param kubernetes Kubernetes auth credentials; mutually exclusive with {@code vaultToken}.
 */
public record VaultCredentialsConfig(
                                     @JsonProperty("vaultToken") @Nullable TokenCredentialsConfig vaultToken,
                                     @JsonProperty("kubernetes") @Nullable KubernetesCredentialsConfig kubernetes) {

    /**
     * Validates that exactly one credential provider is configured.
     */
    public VaultCredentialsConfig {
        if (vaultToken == null && kubernetes == null) {
            throw new IllegalArgumentException("Exactly one of 'vaultToken' or 'kubernetes' credentials must be provided");
        }
        if (vaultToken != null && kubernetes != null) {
            throw new IllegalArgumentException("Exactly one of 'vaultToken' or 'kubernetes' credentials must be provided");
        }
    }
}
