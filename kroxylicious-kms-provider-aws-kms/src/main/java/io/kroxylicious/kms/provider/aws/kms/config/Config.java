/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.aws.kms.config;

import java.net.URI;
import java.security.NoSuchAlgorithmException;
import java.util.Objects;

import javax.net.ssl.SSLContext;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.kroxylicious.proxy.config.secret.PasswordProvider;
import io.kroxylicious.proxy.config.tls.Tls;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Configuration for the Vault KMS service.
 *
 * @param endpointUrl URL of the Vault Transit Engine e.g. {@code https://myhashicorpvault:8200/v1/transit}
 * @param accessKey AWS accessKey
 * @param secretKey the password provider that will provide the Vault token.
 * @param region AWS region
 */

public record Config(
        @JsonProperty(value = "endpointUrl", required = true)
        URI endpointUrl,
        @JsonProperty(required = true)
        PasswordProvider accessKey,
        @JsonProperty(required = true)
        PasswordProvider secretKey,
        @JsonProperty(required = true)
        String region,
        Tls tls
) {
    public Config {
        Objects.requireNonNull(endpointUrl);
        Objects.requireNonNull(region);
        Objects.requireNonNull(accessKey);
        Objects.requireNonNull(secretKey);
    }

    @NonNull
    public SSLContext sslContext() {
        try {
            if (tls == null) {
                return SSLContext.getDefault();
            } else {
                return new JdkTls(tls).sslContext();
            }
        }
        catch (NoSuchAlgorithmException e) {
            throw new SslConfigurationException(e);
        }
    }
}
