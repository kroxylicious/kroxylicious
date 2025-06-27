/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.aws.kms.config;

import java.net.URI;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.kroxylicious.kms.provider.aws.kms.credentials.Credentials;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 *
 * Configuration for the provider that obtains {@link Credentials} from the metadata server of the EC2 instance.
 *
 * @param iamRole name of the IAM role expected to the bound to the EC2 instance (required).
 * @param metadataEndpoint metadata endpoint (defaulted)
 * @param credentialLifetimeFactor the factor applied to determine how long until a credential is preemptively refreshed
 */
public record Ec2MetadataCredentialsProviderConfig(@JsonProperty(value = "iamRole", required = true) String iamRole,
                                                   @JsonProperty(value = "metadataEndpoint", required = false) @Nullable URI metadataEndpoint,
                                                   @JsonProperty(value = "credentialLifetimeFactor", required = false) @Nullable Double credentialLifetimeFactor) {

}