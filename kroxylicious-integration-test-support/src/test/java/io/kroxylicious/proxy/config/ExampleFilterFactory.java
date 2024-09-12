/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.PluginImplConfig;
import io.kroxylicious.proxy.plugin.PluginImplName;
import io.kroxylicious.proxy.plugin.Plugins;

import edu.umd.cs.findbugs.annotations.NonNull;

import static org.junit.jupiter.api.Assertions.fail;

@Plugin(configType = ExampleFilterFactory.Config.class)
public class ExampleFilterFactory implements FilterFactory<ExampleFilterFactory.Config, ExampleFilterFactory.Config> {

    public record Config(
            @PluginImplName(ExamplePluginFactory.class) @JsonProperty(required = true)
            String examplePlugin,
            @PluginImplConfig(implNameProperty = "examplePlugin")
            Object examplePluginConfig
    ) {
    }

    @Override
    public Config initialize(FilterFactoryContext context, Config config) {
        var factory = context.pluginInstance(ExampleFilterFactory.class, config.examplePlugin());
        Objects.requireNonNull(factory, "Violated contract of FilterCreationContext");
        return Plugins.requireConfig(this, config);
    }

    @NonNull
    @Override
    public Filter createFilter(FilterFactoryContext context, Config configuration) {
        fail("unexpected call");
        return null;
    }

}
