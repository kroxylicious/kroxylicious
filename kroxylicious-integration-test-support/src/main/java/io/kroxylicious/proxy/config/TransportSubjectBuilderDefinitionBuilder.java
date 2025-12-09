/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config;

import java.util.Map;

import io.kroxylicious.proxy.authentication.TransportSubjectBuilderService;

public class TransportSubjectBuilderDefinitionBuilder extends AbstractDefinitionBuilder<TransportSubjectBuilderConfig> {
    public TransportSubjectBuilderDefinitionBuilder(String type) {
        super(type);
    }

    @Override
    protected TransportSubjectBuilderConfig buildInternal(String type, Map<String, Object> config) {
        ServiceBasedPluginFactoryRegistry registry = new ServiceBasedPluginFactoryRegistry();
        PluginFactory<TransportSubjectBuilderService> factory = registry.pluginFactory(TransportSubjectBuilderService.class);
        Class<?> configType = factory.configType(type);
        return new TransportSubjectBuilderConfig(type, mapper.convertValue(config, configType));
    }
}
