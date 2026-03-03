/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.hashicorp.vault.model;

import java.util.Set;

import com.fasterxml.jackson.annotation.JsonProperty;

public record CreateTokenRequest(@JsonProperty("display_name") String displayName, @JsonProperty("no_default_policy") boolean noDefaultPolicy, Set<String> policies) {}
