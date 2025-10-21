/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.filter.usernamespace;

import java.util.Objects;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.PluginImplConfig;
import io.kroxylicious.proxy.plugin.PluginImplName;
import io.kroxylicious.proxy.plugin.Plugins;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * A {@link FilterFactory} for {@link UserNamespaceFilter}.
 */
@Plugin(configType = UserNamespace.Config.class)
public class UserNamespace implements FilterFactory<UserNamespace.Config, UserNamespace.Config> {

    private ResourceNameMapper mapper;

    @NonNull
    @Override
    @SuppressWarnings("java:S2638") // Tightening Unknown Nullness
    public Config initialize(FilterFactoryContext context, @NonNull Config config) {
        var configuration = Plugins.requireConfig(this, config);
        ResourceNameMapperService<Object> kmsPlugin = context.pluginInstance(ResourceNameMapperService.class, configuration.mapper());
        kmsPlugin.initialize(configuration.mapperConfig());
        mapper = kmsPlugin.build();
        return config;
    }

    @NonNull
    @Override
    @SuppressWarnings("java:S2638") // Tightening Unknown Nullness
    public UserNamespaceFilter createFilter(FilterFactoryContext context, @NonNull Config configuration) {
        return new UserNamespaceFilter(configuration.resourceTypes, mapper);
    }

    public enum ResourceType {
        TOPIC_NAME,

        GROUP_ID,
        TRANSACTIONAL_ID
    }

    public record Config(@JsonProperty(required = true) Set<ResourceType> resourceTypes,
                         @JsonProperty(required = true) @PluginImplName(ResourceNameMapperService.class) String mapper,
                         @PluginImplConfig(implNameProperty = "mapper") Object mapperConfig) {

        public Config {
            Objects.requireNonNull(resourceTypes);
            if (resourceTypes.contains(ResourceType.TOPIC_NAME)) {
                throw new IllegalArgumentException("Resource type TOPIC_NAME not yet supported by this filter");
            }
        }
    }
}
