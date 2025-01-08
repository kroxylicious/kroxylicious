/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.config;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;

import io.kroxylicious.proxy.plugin.PluginImplConfig;
import io.kroxylicious.proxy.plugin.PluginImplName;
import io.kroxylicious.proxy.service.ClusterNetworkAddressConfigProviderService;

public record ClusterNetworkAddressConfigProviderDefinition(@PluginImplName(ClusterNetworkAddressConfigProviderService.class) String type,
                                                            @PluginImplConfig(implNameProperty = "type") Object config) {

    @JsonCreator
    public ClusterNetworkAddressConfigProviderDefinition {
        Objects.requireNonNull(type);
    }
}
