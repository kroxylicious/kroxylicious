/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.entityisolation;

import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.kroxylicious.proxy.plugin.HasPluginReferences;
import io.kroxylicious.proxy.plugin.PluginReference;

/**
 * Config2 (versioned) configuration for the {@link EntityIsolation} filter.
 * References an {@link EntityNameMapperService} plugin instance by name
 * rather than embedding its configuration inline.
 *
 * @param entityTypes set of entity types to isolate
 * @param mapper name of the EntityNameMapperService plugin instance
 */
public record EntityIsolationConfigV1(
                                      @JsonProperty(required = true) Set<EntityIsolation.EntityType> entityTypes,
                                      @JsonProperty(required = true) String mapper)
        implements HasPluginReferences {

    public EntityIsolationConfigV1 {
        Objects.requireNonNull(entityTypes);
        Objects.requireNonNull(mapper);
        if (entityTypes.contains(EntityIsolation.EntityType.TOPIC_NAME)) {
            throw new IllegalArgumentException("Entity type TOPIC_NAME not yet supported by this filter");
        }
    }

    @Override
    public Stream<PluginReference<?>> pluginReferences() {
        return Stream.of(new PluginReference<>(EntityNameMapperService.class.getName(), mapper));
    }
}
