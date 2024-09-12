/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.config;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.annotation.JsonTypeIdResolver;

public record ClusterNetworkAddressConfigProviderDefinition(
        @JsonProperty(required = true)
        String type,
        @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.EXTERNAL_PROPERTY, property = "type") @JsonTypeIdResolver(
            ClusterNetworkAddressConfigProviderTypeIdResolver.class)
        Object config
) {

    @JsonCreator
    public ClusterNetworkAddressConfigProviderDefinition {
        Objects.requireNonNull(type);
    }
}
