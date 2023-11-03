/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.filter;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.internal.filter.ProduceRequestTransformationFilterFactory.Config;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.PluginImplConfig;
import io.kroxylicious.proxy.plugin.PluginImplName;
import io.kroxylicious.proxy.plugin.Plugins;

import edu.umd.cs.findbugs.annotations.NonNull;

@Plugin(configType = ProduceRequestTransformationFilterFactory.Config.class)
public class ProduceRequestTransformationFilterFactory
        implements FilterFactory<Config, Config> {
    public record Config(
                         @PluginImplName(ByteBufferTransformationFactory.class) @JsonProperty(required = true) String transformation,
                         @PluginImplConfig(implNameProperty = "transformation") Object transformationConfig) {}

    @NonNull
    @Override
    @SuppressWarnings({ "unchecked" })
    public ProduceRequestTransformationFilter createFilter(FilterFactoryContext context,
                                                           Config configuration) {
        var transformation = Plugins.applyConfiguration(context,
                ByteBufferTransformationFactory.class,
                configuration.transformation(),
                configuration.transformationConfig(),
                ByteBufferTransformationFactory::createTransformation);
        return new ProduceRequestTransformationFilter(transformation);
    }

    @Override
    @SuppressWarnings({ "unchecked" })
    public Config initialize(FilterFactoryContext context, Config config) {
        Plugins.requireConfig(this, config);
        Plugins.acceptConfiguration(context,
                ByteBufferTransformationFactory.class,
                config.transformation(),
                config.transformationConfig(),
                ByteBufferTransformationFactory::validateConfiguration);
        return config;
    }

}
