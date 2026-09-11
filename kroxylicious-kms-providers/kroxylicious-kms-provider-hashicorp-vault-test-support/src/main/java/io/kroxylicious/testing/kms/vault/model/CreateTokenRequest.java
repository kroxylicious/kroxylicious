/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.kms.vault.model;

import java.util.Set;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A request to create a Vault token.
 *
 * @param displayName display name for the token.
 * @param noDefaultPolicy if true, the {@code default} policy will not be attached to the token.
 * @param policies names of the policies to attach to the token.
 * @see <a href="https://developer.hashicorp.com/vault/api-docs/auth/token#create-token">Vault API Create Token</a>
 */
public record CreateTokenRequest(@JsonProperty("display_name") String displayName, @JsonProperty("no_default_policy") boolean noDefaultPolicy, Set<String> policies) {}
