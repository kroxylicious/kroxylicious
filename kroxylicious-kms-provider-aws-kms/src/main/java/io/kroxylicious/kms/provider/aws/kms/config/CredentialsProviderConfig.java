/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.aws.kms.config;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import io.kroxylicious.kms.provider.aws.kms.credentials.CredentialsProvider;

import edu.umd.cs.findbugs.annotations.NonNull;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({ @JsonSubTypes.Type(value = LongTermCredentialsProviderConfig.class, name = "longTerm"),
        @JsonSubTypes.Type(value = Ec2MetadataCredentialsProviderConfig.class, name = "ec2Metadata") })
@SuppressWarnings("javaarchitecture:S7027") // Circular dependencies between classes in the same package is caused by use of the JsonSubTypes annotation on this class.
public interface CredentialsProviderConfig {

    @NonNull
    CredentialsProvider createCredentialsProvider();
}
