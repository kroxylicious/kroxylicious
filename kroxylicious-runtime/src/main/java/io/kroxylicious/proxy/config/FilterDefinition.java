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
import io.kroxylicious.proxy.plugin.PluginImplConfig;
import io.kroxylicious.proxy.plugin.PluginImplName;

public record FilterDefinition(
                               @PluginImplName(FilterFactory.class) @JsonProperty(required = true, defaultValue = "") String type,
                               @PluginImplConfig(implNameProperty = "type") Object config) {

    @JsonCreator
    public FilterDefinition {
        Objects.requireNonNull(type);
    }

}
