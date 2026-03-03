/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.filter;

import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Plugin(configType = FactoryMethodConfig.class)
public class FactoryMethod implements FilterFactory<FactoryMethodConfig, String> {
    @Override
    public String initialize(FilterFactoryContext context, FactoryMethodConfig config) throws PluginConfigurationException {
        assertEquals("hello, world", config.str());
        return config.str();
    }

    @Override
    public Filter createFilter(FilterFactoryContext context, String str) {
        throw new RuntimeException("Not expected to be called");
    }
}
