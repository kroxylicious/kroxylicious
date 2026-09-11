/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.subject;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Configuration holder for a mapping rule, either a sed-like replacement rule or the identity fallback.
 *
 * @param replaceMatchMappingRule the sed-like replacement rule, bound from the {@code sedLike} property.
 * @param default_ the identity rule, bound from the {@code default} property.
 */
public record Rule(
                   @JsonProperty("sedLike") ReplaceMatchMappingRule replaceMatchMappingRule,
                   @JsonProperty("default") IdentityMappingRule default_) {}
