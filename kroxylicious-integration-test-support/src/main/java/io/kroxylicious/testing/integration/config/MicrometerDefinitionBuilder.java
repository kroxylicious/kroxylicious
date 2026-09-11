/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.integration.config;

import java.util.Map;

import io.kroxylicious.proxy.config.AbstractDefinitionBuilder;
import io.kroxylicious.proxy.config.MicrometerDefinition;
import io.kroxylicious.proxy.config.PluginFactory;
import io.kroxylicious.proxy.config.ServiceBasedPluginFactoryRegistry;
import io.kroxylicious.proxy.micrometer.MicrometerConfigurationHookService;

/**
 * Builder for {@link MicrometerDefinition}s, resolving the configuration type from the
 * named micrometer configuration hook plugin.
 */
public class MicrometerDefinitionBuilder extends AbstractDefinitionBuilder<MicrometerDefinition> {
    /**
     * Creates a MicrometerDefinitionBuilder.
     *
     * @param type the name of the micrometer configuration hook plugin
     */
    public MicrometerDefinitionBuilder(String type) {
        super(type);
    }

    @Override
    protected MicrometerDefinition buildInternal(String type, Map<String, Object> config) {
        ServiceBasedPluginFactoryRegistry registry = new ServiceBasedPluginFactoryRegistry();
        PluginFactory<MicrometerConfigurationHookService> factory = registry.pluginFactory(MicrometerConfigurationHookService.class);
        Class<?> configType = factory.configType(type);
        return new MicrometerDefinition(type, mapper.convertValue(config, configType));
    }
}
