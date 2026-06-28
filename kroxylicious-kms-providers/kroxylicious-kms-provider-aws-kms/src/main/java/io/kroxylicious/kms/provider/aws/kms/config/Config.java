/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.aws.kms.config;

import java.net.URI;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonProperty.Access;

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
                     @Deprecated(since = "0.21.0", forRemoval = true) @JsonProperty(value = "longTermCredentials", required = false, access = Access.WRITE_ONLY) @Nullable LongTermCredentialsProviderConfig longTermCredentialsProviderConfig,
                     @Deprecated(since = "0.21.0", forRemoval = true) @JsonProperty(value = "ec2MetadataCredentials", required = false, access = Access.WRITE_ONLY) @Nullable Ec2MetadataCredentialsProviderConfig ec2MetadataCredentialsProviderConfig,
                     @JsonProperty(value = "credentials", required = false) @Nullable CredentialsConfig credentials,
                     @JsonProperty(value = "region", required = true) String region,
                     @JsonProperty(value = "tls", required = false) @Nullable Tls tls) {

    public Config {
        Objects.requireNonNull(endpointUrl);
        Objects.requireNonNull(region);

        // Migrate deprecated flat credential fields into the credentials node.
        // Old-style YAML (longTermCredentials / ec2MetadataCredentials at the top level)
        // continues to work but cannot be combined with each other or with the new credentials node.
        // On serialization these deprecated fields are omitted (JsonProperty.Access.WRITE_ONLY),
        // so round-trip through YAML always produces the new 'credentials' form.
        if (longTermCredentialsProviderConfig != null && ec2MetadataCredentialsProviderConfig != null) {
            throw new KmsException(
                    "Cannot specify both deprecated 'longTermCredentials' and 'ec2MetadataCredentials' — use the 'credentials' node with a single provider instead");
        }
        if (credentials != null && (longTermCredentialsProviderConfig != null || ec2MetadataCredentialsProviderConfig != null)) {
            var deprecatedField = longTermCredentialsProviderConfig != null ? "longTermCredentials" : "ec2MetadataCredentials";
            throw new KmsException(
                    "Cannot specify both 'credentials' and deprecated '" + deprecatedField + "' — use the 'credentials' node only");
        }
        if (credentials == null) {
            if (longTermCredentialsProviderConfig != null) {
                credentials = new CredentialsConfig(longTermCredentialsProviderConfig, null, null, null);
            }
            else if (ec2MetadataCredentialsProviderConfig != null) {
                credentials = new CredentialsConfig(null, ec2MetadataCredentialsProviderConfig, null, null);
            }
        }
    }

}
