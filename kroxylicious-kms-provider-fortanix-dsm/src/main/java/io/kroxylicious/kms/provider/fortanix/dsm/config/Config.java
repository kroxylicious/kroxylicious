/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.fortanix.dsm.config;

import java.net.URI;
import java.security.NoSuchAlgorithmException;
import java.util.Objects;

import javax.net.ssl.SSLContext;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.kroxylicious.proxy.config.tls.Tls;
import io.kroxylicious.proxy.tls.JdkTls;
import io.kroxylicious.proxy.tls.SslConfigurationException;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Configuration for the Fortanix DSM KMS service.
 *
 * @param endpointUrl URL of the Fortanix DSM e.g. {@code https://api.uk.smartkey.io}
 * @param apiKeySessionProviderConfig  config for Api Key authentication
 * @param tls tls configuration
 */

public record Config(@JsonProperty(value = "endpointUrl", required = true) URI endpointUrl,
                     @JsonProperty(value = "apiKeySessionProvider") ApiKeySessionProviderConfig apiKeySessionProviderConfig,
                     Tls tls) {
    /**
     *
     * Configuration for the Fortanix DSM KMS service.
     *
     * @param endpointUrl URL of the Fortanix DSM e.g. {@code https://api.uk.smartkey.io}
     * @param apiKeySessionProviderConfig  config for Api Key authentication
     * @param tls tls configuration
     */
    public Config {
        Objects.requireNonNull(endpointUrl);
    }

    /**
     * Creates SSL context for the given configuration.
     *
     * @return SSL context
     */
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
