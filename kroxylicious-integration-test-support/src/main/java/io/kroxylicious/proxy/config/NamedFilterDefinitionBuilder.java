/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config;

import java.util.Map;

import io.kroxylicious.proxy.filter.FilterFactory;

public class NamedFilterDefinitionBuilder extends AbstractDefinitionBuilder<NamedFilterDefinition> {
    private final String name;

    public NamedFilterDefinitionBuilder(String name, String type) {
        super(type);
        this.name = name;
    }

    @Override
    protected NamedFilterDefinition buildInternal(String type, Map<String, Object> config) {
        var configType = new ServiceBasedPluginFactoryRegistry().pluginFactory(FilterFactory.class).configType(type);
        return new NamedFilterDefinition(name, type, mapper.convertValue(config, configType));
    }

    public String name() {
        return name;
    }
}
