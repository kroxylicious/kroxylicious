/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.kms.aws.model;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A request to delete an alias of a KMS key.
 *
 * @param aliasName name of the alias to delete.
 * @see <a href="https://docs.aws.amazon.com/kms/latest/APIReference/API_DeleteAlias.html">AWS API DeleteAlias</a>
 */
public record DeleteAliasRequest(@JsonProperty("AliasName") String aliasName) {
    /**
     * Creates a DeleteAliasRequest.
     *
     * @param aliasName name of the alias to delete.
     */
    public DeleteAliasRequest {
        Objects.requireNonNull(aliasName);
    }
}
