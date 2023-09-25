/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.config;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.annotation.JsonTypeIdResolver;

import io.kroxylicious.proxy.filter.FilterContributor;
import io.kroxylicious.proxy.service.ContributionManager;
import io.kroxylicious.proxy.service.ContributionManager.ConfigurationDefinition;

public record FilterDefinition(@JsonProperty(required = true) String type,
                               @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.EXTERNAL_PROPERTY, property = "type") @JsonTypeIdResolver(FilterConfigTypeIdResolver.class) Object config) {
    @JsonCreator
    public FilterDefinition {
        Objects.requireNonNull(type);
    }

    @JsonIgnore
    public boolean isDefinitionValid() {
        // We could consider allowing Filter authors to supply a config validation function `config -> config.property != null` via the configurationDefinition
        // We would then be able to apply that validation here.
        final ConfigurationDefinition configurationDefinition = ContributionManager.INSTANCE.getDefinition(FilterContributor.class, type);
        return config != null || !configurationDefinition.configurationRequired();
    }
}