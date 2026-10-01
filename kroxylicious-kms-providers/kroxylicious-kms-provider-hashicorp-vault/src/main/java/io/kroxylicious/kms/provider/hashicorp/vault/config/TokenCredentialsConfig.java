/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.hashicorp.vault.config;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.kroxylicious.proxy.config.secret.PasswordProvider;

/**
 * Configuration for authenticating to HashiCorp Vault using a static token.
 *
 * @param token the provider that supplies the Vault client token sent on every request as
 *              the {@code X-Vault-Token} header.
 */
public record TokenCredentialsConfig(
                                     @JsonProperty(value = "token", required = true) PasswordProvider token) {

    /**
     * Validates the record components.
     */
    public TokenCredentialsConfig {
        Objects.requireNonNull(token, "token must not be null");
    }
}
