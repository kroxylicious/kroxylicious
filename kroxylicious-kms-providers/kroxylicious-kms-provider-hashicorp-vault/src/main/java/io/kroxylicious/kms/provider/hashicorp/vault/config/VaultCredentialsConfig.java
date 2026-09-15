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
 *   token:
 *     token:
 *       password: s.myVaultToken
 * }</pre>
 *
 * <p>Example YAML (Kubernetes auth):
 * <pre>{@code
 * credentials:
 *   kubernetes:
 *     role: kroxylicious-vault-role
 * }</pre>
 *
 * @param token      static Vault token credentials; mutually exclusive with {@code kubernetes}.
 * @param kubernetes Kubernetes auth credentials; mutually exclusive with {@code token}.
 */
public record VaultCredentialsConfig(
                                     @JsonProperty("token") @Nullable TokenCredentialsConfig token,
                                     @JsonProperty("kubernetes") @Nullable KubernetesCredentialsConfig kubernetes) {

    /**
     * Validates that exactly one credential provider is configured.
     */
    public VaultCredentialsConfig {
        if (token == null && kubernetes == null) {
            throw new IllegalArgumentException("Either 'token' or 'kubernetes' credentials must be provided");
        }
        if (token != null && kubernetes != null) {
            throw new IllegalArgumentException("Only one of 'token' or 'kubernetes' credentials may be provided");
        }
    }
}
