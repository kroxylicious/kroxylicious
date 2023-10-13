/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public record CompositePrefixingFixedClientIdFilterConfig(String prefix, String clientId) {
    @JsonCreator
    public CompositePrefixingFixedClientIdFilterConfig(@JsonProperty("prefix") String prefix, @JsonProperty("clientId") String clientId) {
        this.prefix = prefix;
        this.clientId = clientId;
    }
}
