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
 */
public record Config(
                     @JsonProperty(value = "vaultTransitEngineUrl", required = true) URI vaultTransitEngineUrl,
                     @JsonProperty(required = true) PasswordProvider vaultToken,
                     @Nullable Tls tls) {
    public Config {
        Objects.requireNonNull(vaultTransitEngineUrl);
        Objects.requireNonNull(vaultToken);
    }

}
