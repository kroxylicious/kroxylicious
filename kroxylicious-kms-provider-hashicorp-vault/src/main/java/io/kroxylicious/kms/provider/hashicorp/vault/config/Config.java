/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.hashicorp.vault.config;

import java.net.URI;
import java.security.NoSuchAlgorithmException;
import java.util.Objects;

import javax.net.ssl.SSLContext;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.kroxylicious.proxy.config.tls.Tls;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Configuration for the Vault KMS service.
 * @param vaultUrl vault url
 * @param vaultToken vault token.
 */
public record Config(
                     @JsonProperty(required = true) URI vaultUrl,
                     @JsonProperty(required = true) String vaultToken,
                     Tls tls) {
    public Config {
        Objects.requireNonNull(vaultUrl);
        Objects.requireNonNull(vaultToken);
    }

    @NonNull
    public SSLContext sslContext() {
        try {
            if (tls == null) {
                return SSLContext.getDefault();
            }
            else {
                return new JdkTls(tls).sslContext();
            }
        }
        catch (NoSuchAlgorithmException e) {
            throw new SslConfigurationException(e);
        }
    }
}
