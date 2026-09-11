/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.integration.config;

import java.util.Map;

import io.kroxylicious.proxy.config.AbstractDefinitionBuilder;
import io.kroxylicious.proxy.config.NamedFilterDefinition;
import io.kroxylicious.proxy.config.ServiceBasedPluginFactoryRegistry;
import io.kroxylicious.proxy.filter.FilterFactory;

/**
 * Builder for {@link NamedFilterDefinition}s, resolving the configuration type from the
 * named filter factory plugin.
 */
public class NamedFilterDefinitionBuilder extends AbstractDefinitionBuilder<NamedFilterDefinition> {
    private final String name;

    /**
     * Creates a NamedFilterDefinitionBuilder.
     *
     * @param name the name to give the filter definition
     * @param type the name of the filter factory plugin
     */
    public NamedFilterDefinitionBuilder(String name, String type) {
        super(type);
        this.name = name;
    }

    @Override
    protected NamedFilterDefinition buildInternal(String type, Map<String, Object> config) {
        var configType = new ServiceBasedPluginFactoryRegistry().pluginFactory(FilterFactory.class).configType(type);
        return new NamedFilterDefinition(name, type, mapper.convertValue(config, configType));
    }

    /**
     * The name that will be given to the built filter definition.
     *
     * @return the filter definition name
     */
    public String name() {
        return name;
    }
}
