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
 * @param longTermCredentialsProviderConfig config for long-term credentials (i.e. access key id and secret access key).
 * @param ec2MetadataCredentialsProviderConfig config obtaining credentials from EC2 metadata
 * @param region AWS region
 * @param tls TLS settings
 */
public record Config(@JsonProperty(value = "endpointUrl", required = true) URI endpointUrl,
                     @JsonProperty(value = "longTermCredentials", required = false) @Nullable LongTermCredentialsProviderConfig longTermCredentialsProviderConfig,
                     @JsonProperty(value = "ec2MetadataCredentials", required = false) @Nullable Ec2MetadataCredentialsProviderConfig ec2MetadataCredentialsProviderConfig,
                     @JsonProperty(value = "region", required = true) String region,
                     @JsonProperty(value = "tls", required = false) @Nullable Tls tls) {

    public Config {
        Objects.requireNonNull(endpointUrl);
        Objects.requireNonNull(region);
    }

}
