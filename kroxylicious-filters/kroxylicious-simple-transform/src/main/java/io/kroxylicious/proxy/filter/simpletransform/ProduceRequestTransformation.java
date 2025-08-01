/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.simpletransform;

import javax.annotation.Nullable;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.filter.simpletransform.ProduceRequestTransformation.Config;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.PluginImplConfig;
import io.kroxylicious.proxy.plugin.PluginImplName;
import io.kroxylicious.proxy.plugin.Plugins;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * A {@link FilterFactory} for {@link ProduceRequestTransformationFilter}.
 *
 */
@Plugin(configType = ProduceRequestTransformation.Config.class)
public class ProduceRequestTransformation
        implements FilterFactory<Config, Config> {
    public record Config(
                         @PluginImplName(ByteBufferTransformationFactory.class) @JsonProperty(required = true) String transformation,
                         @PluginImplConfig(implNameProperty = "transformation") Object transformationConfig) {}

    @Override
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public Filter createFilter(FilterFactoryContext context,
                               @NonNull Config configuration) {
        ByteBufferTransformationFactory factory = context.pluginInstance(ByteBufferTransformationFactory.class, configuration.transformation());
        return new ProduceRequestTransformationFilter(factory.createTransformation(configuration.transformationConfig()));
    }

    @Override
    @SuppressWarnings({ "unchecked" })
    public @NonNull Config initialize(FilterFactoryContext context, @Nullable Config config) {
        var result = Plugins.requireConfig(this, config);
        var transformationFactory = context.pluginInstance(ByteBufferTransformationFactory.class, result.transformation());
        transformationFactory.validateConfiguration(result.transformationConfig());
        return result;
    }

}
