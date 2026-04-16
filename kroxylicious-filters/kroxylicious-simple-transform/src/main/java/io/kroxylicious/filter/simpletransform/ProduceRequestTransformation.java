/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.simpletransform;

import javax.annotation.Nullable;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.plugin.DeprecatedPluginName;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;
import io.kroxylicious.proxy.plugin.PluginImplConfig;
import io.kroxylicious.proxy.plugin.PluginImplName;
import io.kroxylicious.proxy.plugin.Plugins;
import io.kroxylicious.proxy.plugin.ResolvedPluginRegistry;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * A {@link FilterFactory} for {@link ProduceRequestTransformationFilter}.
 *
 */
@Plugin(configType = ProduceRequestTransformation.Config.class)
@Plugin(configVersion = "v1alpha1", configType = ProduceRequestTransformationConfigV1.class)
@DeprecatedPluginName(oldName = "io.kroxylicious.proxy.filter.simpletransform.ProduceRequestTransformation", since = "0.19.0")
public class ProduceRequestTransformation
        implements FilterFactory<Object, ProduceRequestTransformation.Config> {
    public record Config(
                         @PluginImplName(ByteBufferTransformationFactory.class) @JsonProperty(required = true) String transformation,
                         @PluginImplConfig(implNameProperty = "transformation") Object transformationConfig) {}

    @Override
    @SuppressWarnings({ "rawtypes", "unchecked", "java:S2638" })
    public Filter createFilter(FilterFactoryContext context,
                               @NonNull Config configuration) {
        ByteBufferTransformationFactory factory = context.pluginInstance(ByteBufferTransformationFactory.class, configuration.transformation());
        return new ProduceRequestTransformationFilter(factory.createTransformation(configuration.transformationConfig()));
    }

    @Override
    @SuppressWarnings({ "unchecked" })
    public Config initialize(FilterFactoryContext context, @Nullable Object config) {
        var configuration = Plugins.requireConfig(this, config);
        if (configuration instanceof ProduceRequestTransformationConfigV1 v1) {
            return initializeV1(context, v1);
        }
        else if (configuration instanceof Config legacy) {
            return initializeLegacy(context, legacy);
        }
        throw new PluginConfigurationException("Unsupported config type: " + configuration.getClass().getName());
    }

    private Config initializeLegacy(FilterFactoryContext context,
                                    Config config) {
        var transformationFactory = context.pluginInstance(ByteBufferTransformationFactory.class, config.transformation());
        transformationFactory.validateConfiguration(config.transformationConfig());
        return config;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private Config initializeV1(FilterFactoryContext context,
                                ProduceRequestTransformationConfigV1 config) {
        ResolvedPluginRegistry registry = context.resolvedPluginRegistry()
                .orElseThrow(() -> new PluginConfigurationException(
                        "v1alpha1 config requires a ResolvedPluginRegistry but none is available"));
        ByteBufferTransformationFactory transformationFactory = registry.pluginInstance(
                ByteBufferTransformationFactory.class, config.transformation());
        Object transformationConfig = registry.pluginConfig(
                ByteBufferTransformationFactory.class.getName(), config.transformation());
        transformationFactory.validateConfiguration(transformationConfig);
        return new Config(config.transformation(), transformationConfig);
    }

}
