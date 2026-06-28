/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.subject;

import com.fasterxml.jackson.annotation.JsonProperty;

public record Rule(
                   @JsonProperty("sedLike") ReplaceMatchMappingRule replaceMatchMappingRule,
                   @JsonProperty("default") IdentityMappingRule default_) {}
