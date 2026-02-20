/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.entityisolation;

import io.kroxylicious.filter.entityisolation.EntityIsolation.ResourceType;

/**
 * Maps a unmapped java resource name into a mapper one, or vice versa.
 * The {@link #unmap(MapperContext, ResourceType, String)} function must be the reciprocal of
 * the {@link #map(MapperContext, ResourceType, String)}.
 */
public interface EntityNameMapper {
    /**
     * Maps a unmapped resource name into a mapper one.
     *
     * @param mapperContext mapper context.
     * @param resourceType resource type.
     * @param unmappedResourceName unmapped resource name.
     * @return mapped resource name.
     */
    String map(MapperContext mapperContext, ResourceType resourceType,
               String unmappedResourceName);

    /**
     * Maps a mapped resource name back into an unmapped one.
     *
     * @param mapperContext mapper context.
     * @param resourceType resource type.
     * @param mappedResourceName mapped resource name.
     * @return unmapped resource name
     */
    String unmap(MapperContext mapperContext, ResourceType resourceType,
                 String mappedResourceName);

    /**
     * Tests whether the given mapped resource name belongs in this namespace.
     *
     * @param mapperContext mapper context.
     * @param resourceType resource type.
     * @param mappedResourceName mapped resource name.
     * @return true if the mapped resource name belongs in this namespace, false otherwise.
     */
    boolean isInNamespace(MapperContext mapperContext, ResourceType resourceType, String mappedResourceName);

}
