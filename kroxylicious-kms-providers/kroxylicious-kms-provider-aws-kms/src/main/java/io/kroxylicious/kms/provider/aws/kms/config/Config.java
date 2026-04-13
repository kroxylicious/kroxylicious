/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.aws.kms.config;

import java.net.URI;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.kroxylicious.kms.service.KmsException;
import io.kroxylicious.proxy.config.tls.Tls;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Configuration for the AWS KMS service.
 *
 * @param endpointUrl URL of the AWS KMS e.g. {@code https://kms.us-east-1.amazonaws.com}
 * @param longTermCredentialsProviderConfig <b>deprecated</b> — use {@code credentials.longTerm} instead.
 *                                          Config for long-term credentials (access key id and secret access key).
 * @param ec2MetadataCredentialsProviderConfig <b>deprecated</b> — use {@code credentials.ec2Metadata} instead.
 *                                             Config obtaining credentials from EC2 metadata.
 * @param credentials grouped credential provider configuration.
 * @param region AWS region
 * @param tls TLS settings
 */
public record Config(@JsonProperty(value = "endpointUrl", required = true) URI endpointUrl,
                     @JsonProperty(value = "longTermCredentials", required = false) @Nullable LongTermCredentialsProviderConfig longTermCredentialsProviderConfig,
                     @JsonProperty(value = "ec2MetadataCredentials", required = false) @Nullable Ec2MetadataCredentialsProviderConfig ec2MetadataCredentialsProviderConfig,
                     @JsonProperty(value = "credentials", required = false) @Nullable CredentialsConfig credentials,
                     @JsonProperty(value = "region", required = true) String region,
                     @JsonProperty(value = "tls", required = false) @Nullable Tls tls) {

    public Config {
        Objects.requireNonNull(endpointUrl);
        Objects.requireNonNull(region);

        // Migrate deprecated flat credential fields into the credentials node.
        // Old-style YAML (longTermCredentials / ec2MetadataCredentials at the top level)
        // continues to work but cannot be combined with the new credentials node.
        var migrated = credentials;
        if (longTermCredentialsProviderConfig != null) {
            if (migrated != null) {
                throw new KmsException("Cannot specify both 'credentials' and deprecated 'longTermCredentials' — use 'credentials.longTerm' instead");
            }
            migrated = new CredentialsConfig(longTermCredentialsProviderConfig, null, null, null);
        }
        if (ec2MetadataCredentialsProviderConfig != null) {
            if (migrated != null) {
                throw new KmsException("Cannot specify both 'credentials' and deprecated 'ec2MetadataCredentials' — use 'credentials.ec2Metadata' instead");
            }
            migrated = new CredentialsConfig(null, ec2MetadataCredentialsProviderConfig, null, null);
        }
        credentials = migrated;
    }

}
