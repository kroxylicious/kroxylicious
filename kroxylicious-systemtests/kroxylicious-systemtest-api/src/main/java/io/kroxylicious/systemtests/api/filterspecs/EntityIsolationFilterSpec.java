/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.api.filterspecs;

import java.util.Objects;
import java.util.Set;

import io.kroxylicious.systemtests.api.FilterSpec;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * {@link FilterSpec} for the entity-isolation filter.
 * Configures which entity types to isolate and which mapper service to use
 * for mapping principals to entity name prefixes.
 *
 * @param entityTypes       the set of entity types to isolate
 * @param mapperServiceClass the entity name mapper service class
 */
public record EntityIsolationFilterSpec(@NonNull Set<IsolatedEntityType> entityTypes,
                                        @NonNull Class<?> mapperServiceClass)
        implements FilterSpec {

    public EntityIsolationFilterSpec {
        Objects.requireNonNull(entityTypes, "entityTypes must not be null");
        Objects.requireNonNull(mapperServiceClass, "mapperServiceClass must not be null");
        entityTypes = Set.copyOf(entityTypes);
    }
}