/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.fortanix.dsm.config;

import java.net.URI;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.kroxylicious.proxy.config.tls.Tls;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Configuration for the Fortanix DSM KMS service.
 *
 * @param endpointUrl URL of the Fortanix DSM e.g. {@code https://api.uk.smartkey.io}
 * @param apiKeySessionProviderConfig  config for Api Key authentication
 * @param tls tls configuration
 */

public record Config(@JsonProperty(value = "endpointUrl", required = true) URI endpointUrl,
                     @JsonProperty(value = "apiKeySessionProvider") @Nullable ApiKeySessionProviderConfig apiKeySessionProviderConfig,
                     @Nullable Tls tls) {

    private static final Logger LOGGER = LoggerFactory.getLogger(Config.class);

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
        if (!"https".equalsIgnoreCase(endpointUrl.getScheme())) { // string comparison protects against a null schema
            LOGGER.atWarn()
                    .addKeyValue("endpoint", endpointUrl)
                    .log("KMS endpoint does not begin with https://, production installations should use a secure endpoint");
        }
    }

}
