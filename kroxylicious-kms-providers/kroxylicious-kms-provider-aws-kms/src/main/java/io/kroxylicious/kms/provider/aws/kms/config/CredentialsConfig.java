/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.aws.kms.config;

import com.fasterxml.jackson.annotation.JsonProperty;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Groups all AWS credential provider configurations under a single node.
 * Exactly one field must be non-null.
 *
 * @param longTerm long-term IAM credentials (access key + secret key).
 * @param ec2Metadata credentials obtained from the EC2 instance metadata service.
 * @param webIdentity credentials obtained via STS AssumeRoleWithWebIdentity (IRSA on EKS).
 * @param podIdentity credentials obtained from the EKS Pod Identity agent.
 */
public record CredentialsConfig(@JsonProperty("longTerm") @Nullable LongTermCredentialsProviderConfig longTerm,
                                @JsonProperty("ec2Metadata") @Nullable Ec2MetadataCredentialsProviderConfig ec2Metadata,
                                @JsonProperty("webIdentity") @Nullable WebIdentityCredentialsProviderConfig webIdentity,
                                @JsonProperty("podIdentity") @Nullable PodIdentityCredentialsProviderConfig podIdentity) {

}
