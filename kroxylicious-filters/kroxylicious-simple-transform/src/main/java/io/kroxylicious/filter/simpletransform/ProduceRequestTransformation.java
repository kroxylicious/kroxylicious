/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.simpletransform;

import javax.annotation.Nullable;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.kroxylicious.filter.simpletransform.ProduceRequestTransformation.Config;
import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.plugin.DeprecatedPluginName;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;
import io.kroxylicious.proxy.plugin.PluginImplConfig;
import io.kroxylicious.proxy.plugin.PluginImplName;
import io.kroxylicious.proxy.plugin.Plugins;
import io.kroxylicious.proxy.plugin.UnknownPluginInstanceException;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * A {@link FilterFactory} for {@link ProduceRequestTransformationFilter}.
 *
 */
@Plugin(configType = ProduceRequestTransformation.Config.class)
@DeprecatedPluginName(oldName = "io.kroxylicious.proxy.filter.simpletransform.ProduceRequestTransformation", since = "0.19.0")
public class ProduceRequestTransformation
        implements FilterFactory<Config, Config> {
    public record Config(
                         @PluginImplName(ByteBufferTransformationFactory.class) @JsonProperty(required = true) String transformation,
                         @PluginImplConfig(implNameProperty = "transformation") Object transformationConfig) {}

    @Override
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public Filter createFilter(FilterFactoryContext context,
                               @NonNull Config configuration) {
        ByteBufferTransformationFactory factory;
        try {
            factory = context.pluginInstance(ByteBufferTransformationFactory.class, configuration.transformation());
        }
        catch (UnknownPluginInstanceException e) {
            io.kroxylicious.proxy.filter.simpletransform.ByteBufferTransformationFactory oldFactory = context
                    .pluginInstance(io.kroxylicious.proxy.filter.simpletransform.ByteBufferTransformationFactory.class, configuration.transformation());
            factory = new ByteBufferTransformationFactory() {
                @Override
                public void validateConfiguration(Object config) throws PluginConfigurationException {
                    oldFactory.validateConfiguration(config);
                }

                @Override
                public ByteBufferTransformation createTransformation(Object configuration) {
                    return oldFactory.createTransformation(configuration);
                }
            };
        }
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
