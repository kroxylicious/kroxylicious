/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.kroxylicious.proxy.config.BaseConfig;

public class CombinedConfiguration extends BaseConfig {

    private final String prefix;
    private final String fixedClientId;

    @JsonCreator
    public CombinedConfiguration(@JsonProperty("prefix") String prefix, @JsonProperty("fixedClientId") String fixedClientId) {
        this.prefix = prefix;
        this.fixedClientId = fixedClientId;
    }

    public String getPrefix() {
        return prefix;
    }

    public String getFixedClientId() {
        return fixedClientId;
    }
}
