/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.kms.aws.model;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A request to create an alias for a KMS key.
 *
 * @param targetKeyId id of the key to associate with the alias.
 * @param aliasName name of the alias.
 * @see <a href="https://docs.aws.amazon.com/kms/latest/APIReference/API_CreateAlias.html">AWS API CreateAlias</a>
 */
public record CreateAliasRequest(@JsonProperty("TargetKeyId") String targetKeyId,
                                 @JsonProperty("AliasName") String aliasName) {
    /**
     * Creates a CreateAliasRequest.
     *
     * @param targetKeyId id of the key to associate with the alias.
     * @param aliasName name of the alias.
     */
    public CreateAliasRequest {
        Objects.requireNonNull(targetKeyId);
        Objects.requireNonNull(aliasName);
    }
}
