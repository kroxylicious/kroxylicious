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

import io.kroxylicious.kms.provider.aws.kms.credentials.CredentialsProvider;
import io.kroxylicious.kms.provider.aws.kms.credentials.Ec2CredentialsProvider;

/**
 * Config for the EC2 Credentials Provider.
 *
 * @param ec2IamRole name of the IAM role expected to the bound to the EC2 instance (required).
 * @param metadataEndpoint metadata endpoint (defaulted)
 * @param credentialLifetimeFactor the factor applied to determine how long until a credential is preemptively refreshed
 */
public record Ec2CredentialsProviderConfig(@JsonProperty(value = "ec2IamRole", required = true) String ec2IamRole,
                                           @JsonProperty(value = "metadataEndpoint", required = false) Optional<URI> metadataEndpoint,
                                           @JsonProperty(value = "credentialLifetimeFactor", required = false) Optional<Double> credentialLifetimeFactor)
        implements CredentialsProviderConfig {
    public Ec2CredentialsProviderConfig {
        Objects.requireNonNull(ec2IamRole);
    }

    @Override
    public CredentialsProvider createCredentialsProvider() {
        return new Ec2CredentialsProvider(this);
    }

    @Override
    public String toString() {
        return "Ec2CredentialsProviderConfig[" +
                "metadataEndpoint=" + metadataEndpoint.map(URI::toString).orElse("<default>") + ',' +
                "credentialLifetimeFactor=" + credentialLifetimeFactor.map(String::valueOf).orElse("<default>") + ',' +
                "ec2IamRole=" + ec2IamRole + ']';
    }
}
