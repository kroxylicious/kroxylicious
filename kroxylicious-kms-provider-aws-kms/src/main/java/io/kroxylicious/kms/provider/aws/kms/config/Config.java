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
 * @param endpointUrl URL of the AWS KMS e.g. {@code https://kms.us-east1.amazonaws.com}
 * @param credentialsProvider AWS credentials provider
 * @param region AWS region e.g. us-east1
 * @param tls TLS
 */
public record Config(@JsonProperty(value = "endpointUrl", required = true) URI endpointUrl,
                     @JsonProperty(value = "credentialsProvider", required = true) CredentialsProviderConfig<?> credentialsProvider,
                     @JsonProperty(required = true) String region,
                     Tls tls) {

    public Config {
        Objects.requireNonNull(endpointUrl);
        Objects.requireNonNull(credentialsProvider);
        Objects.requireNonNull(region);
    }

    /**
     * Configuration for the AWS KMS service.
     *
     * @param endpointUrl URL of the AWS KMS e.g. {@code https://kms.us-east1.amazonaws.com}
     * @param accessKey AWS accessKey
     * @param secretKey AWS secretKey
     * @param region AWS region
     * @param tls TLS
     *
     * @deprecated use {@link Config#Config(URI, CredentialsProviderConfig, String, Tls)}
     */
    @Deprecated(forRemoval = true, since = "0.7.0")
    public Config(@JsonProperty(value = "endpointUrl", required = true) URI endpointUrl,
                  @JsonProperty(required = true) PasswordProvider accessKey,
                  @JsonProperty(required = true) PasswordProvider secretKey,
                  @JsonProperty(required = true) String region,
                  Tls tls) {
        this(endpointUrl, new AccessAndSecretKeyTupleCredentialsProviderConfig(accessKey, secretKey), region, tls);
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
