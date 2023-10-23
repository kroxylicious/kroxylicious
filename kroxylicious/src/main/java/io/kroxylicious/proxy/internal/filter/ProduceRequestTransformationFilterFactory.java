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
import io.kroxylicious.proxy.plugin.PluginConfigType;
import io.kroxylicious.proxy.plugin.PluginImplConfig;
import io.kroxylicious.proxy.plugin.PluginImplName;
import io.kroxylicious.proxy.plugin.Plugins;

@PluginConfigType(ProduceRequestTransformationFilterFactory.Config.class)
public class ProduceRequestTransformationFilterFactory
        implements FilterFactory<Config, Config> {
    public record Config(
                         @PluginImplName(ByteBufferTransformationFactory.class) @JsonProperty(required = true) String transformation,
                         @PluginImplConfig(implNameProperty = "transformation") Object transformationConfig) {}

    @Override
    public ProduceRequestTransformationFilter createFilter(FilterFactoryContext context,
                                                           Config configuration) {
        @SuppressWarnings({"unchecked", "rawtypes"})
        Class<ByteBufferTransformationFactory<?>> pluginClass = (Class) ByteBufferTransformationFactory.class;
        ByteBufferTransformationFactory<?> factory = context.pluginInstance(pluginClass, configuration.transformation());
        return new ProduceRequestTransformationFilter(factory.createTransformation(configuration.transformationConfig()));
    }

    @Override
    public Config initialize(FilterFactoryContext context, Config config) {
        Plugins.requireConfig(this, config);
        var transformationFactory = context.pluginInstance(ByteBufferTransformationFactory.class, config.transformation());
        transformationFactory.validateConfiguration(config.transformationConfig());
        return config;
    }

}
