/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.aws.kms.config;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import io.kroxylicious.kms.provider.aws.kms.credentials.CredentialsProvider;

@JsonTypeInfo(use = JsonTypeInfo.Id.DEDUCTION)
@JsonSubTypes({ @JsonSubTypes.Type(AccessAndSecretKeyTupleCredentialsProviderConfig.class), @JsonSubTypes.Type(Ec2CredentialsProviderConfig.class) })
public interface CredentialsProviderConfig<C extends CredentialsProviderConfig<?>> {

    CredentialsProvider createCredentialsProvider();
}
