/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.simpletransform;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.filter.simpletransform.FetchResponseTransformationFilterFactory.Config;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.PluginImplConfig;
import io.kroxylicious.proxy.plugin.PluginImplName;
import io.kroxylicious.proxy.plugin.Plugins;

import edu.umd.cs.findbugs.annotations.NonNull;

@Plugin(configType = FetchResponseTransformationFilterFactory.Config.class)
public class FetchResponseTransformationFilterFactory
                                                      implements FilterFactory<Config, Config> {

    @Override
    public Config initialize(FilterFactoryContext context, Config config) {
        return Plugins.requireConfig(this, config);
    }

    @NonNull
    @Override
    public FetchResponseTransformationFilter createFilter(
            FilterFactoryContext context,
            Config configuration
    ) {
        var factory = context.pluginInstance(ByteBufferTransformationFactory.class, configuration.transformation());
        Objects.requireNonNull(factory, "Violated contract of FilterCreationContext");
        return new FetchResponseTransformationFilter(factory.createTransformation(configuration.transformationConfig()));
    }

    public record Config(
            @JsonProperty(required = true) @PluginImplName(ByteBufferTransformationFactory.class)
            String transformation,
            @PluginImplConfig(implNameProperty = "transformation")
            Object transformationConfig
    ) {

    }

}
