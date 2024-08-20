/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.aws.kms.config;

import java.net.URI;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.kroxylicious.kms.provider.aws.kms.credentials.CredentialsProvider;
import io.kroxylicious.kms.provider.aws.kms.credentials.Ec2CredentialsProvider;

public record Ec2CredentialsProviderConfig(@JsonProperty(value = "iamRole", required = true) String iamRole,
                                           @JsonProperty(value = "metadataEndpoint", required = false) URI metadataEndpoint)
        implements CredentialsProviderConfig<Ec2CredentialsProviderConfig> {
    public Ec2CredentialsProviderConfig {
        Objects.requireNonNull(iamRole);
    }

    @Override
    public CredentialsProvider createCredentialsProvider() {
        return new Ec2CredentialsProvider(this);
    }

    @Override
    public String toString() {
        return "Ec2CredentialsProviderConfig[" +
                "metadataEndpoint=" + metadataEndpoint + ',' +
                "iamRole=" + iamRole + ']';
    }
}
