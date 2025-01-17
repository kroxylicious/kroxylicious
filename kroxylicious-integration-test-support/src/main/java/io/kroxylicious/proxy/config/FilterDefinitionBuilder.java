/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config;

import java.util.Map;

import io.kroxylicious.proxy.filter.FilterFactory;

@Deprecated
public class FilterDefinitionBuilder extends AbstractDefinitionBuilder<FilterDefinition> {
    public FilterDefinitionBuilder(String type) {
        super(type);
    }

    @Override
    protected FilterDefinition buildInternal(String type, Map<String, Object> config) {
        var configType = new ServiceBasedPluginFactoryRegistry().pluginFactory(FilterFactory.class).configType(type);
        return new FilterDefinition(type, mapper.convertValue(config, configType));
    }
}
