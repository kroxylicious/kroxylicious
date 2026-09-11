/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.filter;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.PluginImplConfig;
import io.kroxylicious.proxy.plugin.PluginImplName;
import io.kroxylicious.proxy.plugin.Plugins;

import static org.junit.jupiter.api.Assertions.fail;

@Plugin(configType = NestedPluginConfigFactory.NestedPluginConfig.class)
public class NestedPluginConfigFactory implements FilterFactory<NestedPluginConfigFactory.NestedPluginConfig, NestedPluginConfigFactory.NestedPluginConfig> {

    @Override
    public NestedPluginConfig initialize(FilterFactoryContext context, NestedPluginConfig config) {
        var factory = context.pluginInstance(ExamplePluginFactory.class, config.examplePlugin());
        Objects.requireNonNull(factory, "Violated contract of FilterCreationContext");
        return Plugins.requireConfig(this, config);
    }

    @Override
    public Filter createFilter(FilterFactoryContext context, NestedPluginConfig configuration) {
        fail("unexpected call");
        return null;
    }

    public record NestedPluginConfig(@PluginImplName(ExamplePluginFactory.class) @JsonProperty(required = true) String examplePlugin,
                                     @PluginImplConfig(implNameProperty = "examplePlugin") Object examplePluginConfig) {}

}
