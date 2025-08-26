/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.azure.config;

import java.net.URI;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import io.kroxylicious.kms.provider.azure.config.auth.EntraIdentityConfig;
import io.kroxylicious.proxy.config.tls.Tls;

import edu.umd.cs.findbugs.annotations.Nullable;

@JsonPropertyOrder({ "entraIdentity", "keyVaultBaseUrl", "tls" })
public record AzureKeyVaultConfig(@JsonProperty(required = true) EntraIdentityConfig entraIdentity,
                                  @JsonProperty(required = true) URI keyVaultBaseUrl,
                                  @JsonInclude(JsonInclude.Include.NON_NULL) @JsonProperty(value = "tls") @Nullable Tls tls) {

    private static final Logger LOG = LoggerFactory.getLogger(AzureKeyVaultConfig.class);

    public AzureKeyVaultConfig {
        Objects.requireNonNull(entraIdentity);
        Objects.requireNonNull(keyVaultBaseUrl);
        if (!keyVaultBaseUrl.getScheme().equals("https")) {
            LOG.warn("keyVaultBaseUrl {} does not begin with https://, production installations should use a secure endpoint", keyVaultBaseUrl);
        }
    }
}
