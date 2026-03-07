/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.filter.entityisolation;

import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import org.apache.kafka.common.config.ConfigResource;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.PluginImplConfig;
import io.kroxylicious.proxy.plugin.PluginImplName;
import io.kroxylicious.proxy.plugin.Plugins;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * A {@link FilterFactory} for {@link EntityIsolationFilter}.
 */
@Plugin(configType = EntityIsolation.Config.class)
public class EntityIsolation implements FilterFactory<EntityIsolation.Config, EntityIsolation.Config> {

    @Nullable
    private EntityNameMapper mapper;

    @NonNull
    @Override
    @SuppressWarnings("java:S2638") // Tightening Unknown Nullness
    public Config initialize(FilterFactoryContext context, @NonNull Config config) {
        var configuration = Plugins.requireConfig(this, config);
        EntityNameMapperService<Object> mapperService = context.pluginInstance(EntityNameMapperService.class, configuration.mapper());
        mapperService.initialize(configuration.mapperConfig());
        mapper = mapperService.build();
        return config;
    }

    @NonNull
    @Override
    @SuppressWarnings("java:S2638") // Tightening Unknown Nullness
    public EntityIsolationFilter createFilter(FilterFactoryContext context, @NonNull Config configuration) {
        Objects.requireNonNull(configuration, "configuration must not be null");
        if (mapper == null) {
            throw new IllegalStateException("filter factory has not been initialized");
        }
        return new EntityIsolationFilter(configuration.resourceTypes, mapper);
    }

    public enum ResourceType {
        TOPIC_NAME,
        GROUP_ID,
        TRANSACTIONAL_ID
    }

    public record Config(@JsonProperty(required = true) Set<ResourceType> resourceTypes,
                         @JsonProperty(required = true) @PluginImplName(EntityNameMapperService.class) String mapper,
                         @PluginImplConfig(implNameProperty = "mapper") Object mapperConfig) {

        public Config {
            Objects.requireNonNull(resourceTypes);
            if (resourceTypes.contains(ResourceType.TOPIC_NAME)) {
                throw new IllegalArgumentException("Resource type TOPIC_NAME not yet supported by this filter");
            }
        }
    }

    static Optional<EntityIsolation.ResourceType> fromKafkaResourceTypeCode(byte resourceType) {
        return Optional.of(resourceType)
                .map(org.apache.kafka.common.resource.ResourceType::fromCode)
                .flatMap(EntityIsolation::fromKafkaResourceType);
    }

    static Optional<ResourceType> fromKafkaResourceType(org.apache.kafka.common.resource.ResourceType resourceType) {
        return switch (resourceType) {
            case TOPIC -> Optional.of(ResourceType.TOPIC_NAME);
            case GROUP -> Optional.of(ResourceType.GROUP_ID);
            case TRANSACTIONAL_ID -> Optional.of(ResourceType.TRANSACTIONAL_ID);
            default -> Optional.empty();
        };
    }

    static Optional<EntityIsolation.ResourceType> fromConfigResourceTypeCode(byte resourceType) {
        return Optional.of(resourceType)
                .map(ConfigResource.Type::forId)
                .flatMap(EntityIsolation::fromConfigResourceType);
    }

    static Optional<ResourceType> fromConfigResourceType(ConfigResource.Type resourceType) {
        return switch (resourceType) {
            case TOPIC -> Optional.of(ResourceType.TOPIC_NAME);
            case GROUP -> Optional.of(ResourceType.GROUP_ID);
            default -> Optional.empty();
        };
    }

}
