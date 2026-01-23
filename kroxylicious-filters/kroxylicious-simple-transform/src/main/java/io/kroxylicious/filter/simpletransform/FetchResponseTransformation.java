/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.simpletransform;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.kroxylicious.filter.simpletransform.FetchResponseTransformation.Config;
import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.plugin.DeprecatedPluginName;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.PluginImplConfig;
import io.kroxylicious.proxy.plugin.PluginImplName;
import io.kroxylicious.proxy.plugin.Plugins;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * A {@link FilterFactory} for {@link FetchResponseTransformationFilter}.
 */
@Plugin(configType = FetchResponseTransformation.Config.class)
@DeprecatedPluginName(oldName = "io.kroxylicious.proxy.filter.simpletransform.FetchResponseTransformation", since = "0.19.0")
public class FetchResponseTransformation implements FilterFactory<Config, Config> {

    @Override
    public Config initialize(FilterFactoryContext context, @Nullable Config config) {
        return Plugins.requireConfig(this, config);
    }

    @Override
    @SuppressWarnings({ "rawtypes", "unchecked", "java:S2638" })
    public Filter createFilter(FilterFactoryContext context,
                               @NonNull Config configuration) {
        var factory = context.pluginInstance(ByteBufferTransformationFactory.class, configuration.transformation());
        Objects.requireNonNull(factory, "Violated contract of FilterCreationContext");
        return new FetchResponseTransformationFilter(factory.createTransformation(configuration.transformationConfig()));
    }

    public record Config(@JsonProperty(required = true) @PluginImplName(ByteBufferTransformationFactory.class) String transformation,
                         @PluginImplConfig(implNameProperty = "transformation") Object transformationConfig) {

    }

}
