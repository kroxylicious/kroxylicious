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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.kroxylicious.proxy.config.secret.PasswordProvider;
import io.kroxylicious.proxy.config.tls.Tls;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Configuration for the AWS KMS service.
 *
 * @param endpointUrl URL of the AWS KMS e.g. {@code https://kms.us-east-1.amazonaws.com}
 * @param credentialsProvider AWS credentials provider
 * @param region AWS region e.g. us-east-1
 * @param tls TLS
 */
public record Config(@JsonProperty(value = "endpointUrl", required = true) URI endpointUrl,
                     @JsonProperty(value = "credentialsProvider", required = false) CredentialsProviderConfig credentialsProvider,
                     @JsonProperty(value = "region", required = true) String region,
                     @JsonProperty(value = "tls", required = false) Tls tls) {

    private static final Logger LOGGER = LoggerFactory.getLogger(Config.class);

    public Config {
        Objects.requireNonNull(endpointUrl);
        Objects.requireNonNull(credentialsProvider);
        Objects.requireNonNull(region);
    }

    /**
     * Configuration for the AWS KMS service.
     *
     * @param endpointUrl URL of the AWS KMS e.g. {@code https://kms.us-east-1.amazonaws.com}
     * @param accessKey AWS accessKey
     * @param secretKey AWS secretKey
     * @param region AWS region e.g. us-east-1
     * @param tls TLS
     *
     * @deprecated use {@link Config#Config(URI, CredentialsProviderConfig, String, Tls)}
     */
    @Deprecated(forRemoval = true, since = "0.8.0")
    private Config(@JsonProperty(value = "endpointUrl", required = true) URI endpointUrl,
                   @JsonProperty(value = "accessKey", required = true) PasswordProvider accessKey,
                   @JsonProperty(value = "secretKey", required = true) PasswordProvider secretKey,
                   @JsonProperty(value = "region", required = true) String region,
                   @JsonProperty(value = "tls", required = false) Tls tls) {
        this(endpointUrl, new FixedCredentialsProviderConfig(accessKey, secretKey), region, tls);
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

    @JsonCreator
    @SuppressWarnings("removal")
    public static Config create(@JsonProperty(value = "endpointUrl", required = true) URI endpointUrl,
                                @JsonProperty(value = "accessKey", required = false) PasswordProvider accessKey,
                                @JsonProperty(value = "secretKey", required = false) PasswordProvider secretKey,
                                @JsonProperty(value = "credentialsProvider", required = false) CredentialsProviderConfig credentialsProvider,
                                @JsonProperty(value = "region", required = true) String region,
                                @JsonProperty(value = "tls", required = false) Tls tls) {
        if (credentialsProvider != null) {
            if (accessKey != null) {
                throw new IllegalArgumentException("Cannot provide accessKey when using credentialsProvider");
            }
            if (secretKey != null) {
                throw new IllegalArgumentException("Cannot provide secretKey when using credentialsProvider");
            }
            return new Config(endpointUrl, credentialsProvider, region, tls);
        }
        else {
            LOGGER.warn("Use of deprecated accessKey and secretKey. Use credentialsProvider type fixed to configure long-lived credentials.");
            return new Config(endpointUrl, accessKey, secretKey, region, tls);
        }
    }

}
