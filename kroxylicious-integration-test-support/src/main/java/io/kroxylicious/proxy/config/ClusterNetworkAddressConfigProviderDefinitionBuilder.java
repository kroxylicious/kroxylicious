/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config;

import java.util.Map;

import io.kroxylicious.proxy.service.ClusterNetworkAddressConfigProviderService;

public class ClusterNetworkAddressConfigProviderDefinitionBuilder extends AbstractDefinitionBuilder<ClusterNetworkAddressConfigProviderDefinition> {
    public ClusterNetworkAddressConfigProviderDefinitionBuilder(String type) {
        super(type);
    }

    @Override
    protected ClusterNetworkAddressConfigProviderDefinition buildInternal(String type, Map<String, Object> config) {
        Class<?> configType = new ServiceBasedPluginFactoryRegistry().pluginFactory(ClusterNetworkAddressConfigProviderService.class).configType(type);
        return new ClusterNetworkAddressConfigProviderDefinition(type, mapper.convertValue(config, configType));
    }
}
