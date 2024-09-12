/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.simpletransform;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.filter.simpletransform.ProduceRequestTransformationFilterFactory.Config;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.PluginImplConfig;
import io.kroxylicious.proxy.plugin.PluginImplName;
import io.kroxylicious.proxy.plugin.Plugins;

import edu.umd.cs.findbugs.annotations.NonNull;

@Plugin(configType = ProduceRequestTransformationFilterFactory.Config.class)
public class ProduceRequestTransformationFilterFactory
                                                       implements FilterFactory<Config, Config> {
    public record Config(
            @PluginImplName(ByteBufferTransformationFactory.class) @JsonProperty(required = true)
            String transformation,
            @PluginImplConfig(implNameProperty = "transformation")
            Object transformationConfig
    ) {
    }

    @NonNull
    @Override
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public ProduceRequestTransformationFilter createFilter(
            FilterFactoryContext context,
            Config configuration
    ) {
        ByteBufferTransformationFactory factory = context.pluginInstance(ByteBufferTransformationFactory.class, configuration.transformation());
        return new ProduceRequestTransformationFilter(factory.createTransformation(configuration.transformationConfig()));
    }

    @Override
    @SuppressWarnings({ "unchecked" })
    public Config initialize(FilterFactoryContext context, Config config) {
        Plugins.requireConfig(this, config);
        var transformationFactory = context.pluginInstance(ByteBufferTransformationFactory.class, config.transformation());
        transformationFactory.validateConfiguration(config.transformationConfig());
        return config;
    }

}
