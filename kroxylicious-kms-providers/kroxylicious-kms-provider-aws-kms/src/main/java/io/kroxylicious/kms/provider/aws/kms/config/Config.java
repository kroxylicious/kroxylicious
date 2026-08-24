/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.aws.kms.config;

import java.net.URI;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.kroxylicious.proxy.config.tls.Tls;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Configuration for the AWS KMS service.
 *
 * @param endpointUrl URL of the AWS KMS e.g. {@code https://kms.us-east-1.amazonaws.com}
 * @param credentials grouped credential provider configuration.
 * @param region AWS region
 * @param tls TLS settings
 */
public record Config(@JsonProperty(value = "endpointUrl", required = true) URI endpointUrl,
                     @JsonProperty(value = "credentials", required = true) CredentialsConfig credentials,
                     @JsonProperty(value = "region", required = true) String region,
                     @JsonProperty(value = "tls", required = false) @Nullable Tls tls) {

    public Config {
        Objects.requireNonNull(endpointUrl);
        Objects.requireNonNull(credentials);
        Objects.requireNonNull(region);
    }

}
