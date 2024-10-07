/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.aws.kms.config;

import java.net.URI;
import java.util.Objects;
import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

import io.kroxylicious.kms.provider.aws.kms.credentials.Credentials;
import io.kroxylicious.kms.provider.aws.kms.credentials.CredentialsProvider;
import io.kroxylicious.kms.provider.aws.kms.credentials.Ec2MetadataCredentialsProvider;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 *
 * Configuration for the provider that obtains {@link Credentials} from the metadata server of the EC2 instance.
 *
 * @param iamRole name of the IAM role expected to the bound to the EC2 instance (required).
 * @param metadataEndpoint metadata endpoint (defaulted)
 * @param credentialLifetimeFactor the factor applied to determine how long until a credential is preemptively refreshed
 */
@JsonTypeName("ec2Metadata")
public record Ec2MetadataCredentialsProviderConfig(@JsonProperty(value = "iamRole", required = true) String iamRole,
                                                   @JsonProperty(value = "metadataEndpoint", required = false) Optional<URI> metadataEndpoint,
                                                   @JsonProperty(value = "credentialLifetimeFactor", required = false) Optional<Double> credentialLifetimeFactor)
        implements CredentialsProviderConfig {
    public Ec2MetadataCredentialsProviderConfig {
        Objects.requireNonNull(iamRole);
    }

    @Override
    public @NonNull CredentialsProvider createCredentialsProvider() {
        return new Ec2MetadataCredentialsProvider(this);
    }

    @Override
    public String toString() {
        return "Ec2CredentialsProviderConfig[" +
                "metadataEndpoint=" + metadataEndpoint.map(URI::toString).orElse("<default>") + ',' +
                "credentialLifetimeFactor=" + credentialLifetimeFactor.map(String::valueOf).orElse("<default>") + ',' +
                "iamRole=" + iamRole + ']';
    }
}
