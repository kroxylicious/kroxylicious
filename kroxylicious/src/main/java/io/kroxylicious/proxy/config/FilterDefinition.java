/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.config;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.plugin.PluginConfig;
import io.kroxylicious.proxy.plugin.PluginReference;

public record FilterDefinition(
                               @PluginReference(FilterFactory.class) @JsonProperty(required = true) String type,
                               @PluginConfig(instanceNameProperty = "type") Object config) {

    @JsonCreator
    public FilterDefinition {
        Objects.requireNonNull(type);
    }

}
