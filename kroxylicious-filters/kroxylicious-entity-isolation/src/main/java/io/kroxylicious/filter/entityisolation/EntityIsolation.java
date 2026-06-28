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
import org.apache.kafka.common.protocol.ApiKeys;

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
 * Entity isolation gives a user a private space within a Kafka cluster that is isolated from other users sharing the same cluster
 * for some of their entities.  Isolation can be applied selectively so that isolation is applied to some entity types
 * and not others.
 * <br/>
 * Currently, entity isolation is limited to groupId and transactionalId entity types.
 * <br/>
 * A {@link FilterFactory} for {@link EntityIsolationFilter}.
 */
@Plugin(configType = EntityIsolation.Config.class)
public class EntityIsolation implements FilterFactory<EntityIsolation.Config, EntityIsolation.Config> {

    @Nullable
    private EntityNameMapper mapper;

    /**
     * Create the entity isolation factory.
     */
    public EntityIsolation() {
        // empty
    }

    @NonNull
    @Override
    @SuppressWarnings({ "java:S2638", "unchecked" }) // Tightening Unknown Nullness
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
        return new EntityIsolationFilter(configuration.entityTypes, mapper);
    }

    /**
     * The kafka entity types that can be isolated.
     */
    public enum EntityType {
        /**
         * Topic name
         */
        TOPIC_NAME,
        /**
         * Group name
         */
        GROUP_ID,
        /**
         * Transactional id
         */
        TRANSACTIONAL_ID
    }

    /**
     * Configuration for the {@link EntityIsolation}.
     *
     * @param entityTypes set of entity types to isolated.
     * @param mapper mapper name
     * @param mapperConfig mapper config
     */
    public record Config(@JsonProperty(required = true) Set<EntityType> entityTypes,
                         @JsonProperty(required = true) @PluginImplName(EntityNameMapperService.class) String mapper,
                         @PluginImplConfig(implNameProperty = "mapper") Object mapperConfig) {

        public Config {
            Objects.requireNonNull(entityTypes);
            Objects.requireNonNull(mapper);
            if (entityTypes.contains(EntityType.TOPIC_NAME)) {
                throw new IllegalArgumentException("Entity type TOPIC_NAME not yet supported by this filter");
            }
        }
    }

    /**
     * Decodes type codes used by various Kafka RPCs
     *
     * @param apiKey api key
     * @param resourceTypeCode resource type code
     * @return entity type
     */
    static Optional<EntityType> fromResourceTypeCode(ApiKeys apiKey, byte resourceTypeCode) {
        return switch (apiKey) {
            case INCREMENTAL_ALTER_CONFIGS, ALTER_CONFIGS, DESCRIBE_CONFIGS, LIST_CONFIG_RESOURCES -> fromConfigResourceType(resourceTypeCode);
            case CREATE_ACLS, DELETE_ACLS, DESCRIBE_ACLS -> fromAclResourceType(resourceTypeCode);
            default -> throw new IllegalArgumentException("Unable to decode resourceType (%d) for %s".formatted(resourceTypeCode, apiKey));
        };
    }

    private static Optional<EntityType> fromConfigResourceType(byte resourceType) {
        return Optional.of(resourceType)
                .map(ConfigResource.Type::forId)
                .flatMap(EntityIsolation::fromConfigResourceType);
    }

    private static Optional<EntityType> fromAclResourceType(byte resourceType) {
        return Optional.of(resourceType)
                .map(org.apache.kafka.common.resource.ResourceType::fromCode)
                .flatMap(EntityIsolation::fromAclResourceType);
    }

    private static Optional<EntityType> fromConfigResourceType(ConfigResource.Type resourceType) {
        return switch (resourceType) {
            case TOPIC -> Optional.of(EntityType.TOPIC_NAME);
            case GROUP -> Optional.of(EntityType.GROUP_ID);
            default -> Optional.empty();
        };
    }

    private static Optional<EntityType> fromAclResourceType(org.apache.kafka.common.resource.ResourceType resourceType) {
        return switch (resourceType) {
            case TOPIC -> Optional.of(EntityType.TOPIC_NAME);
            case GROUP -> Optional.of(EntityType.GROUP_ID);
            case TRANSACTIONAL_ID -> Optional.of(EntityType.TRANSACTIONAL_ID);
            default -> Optional.empty();
        };
    }
}
