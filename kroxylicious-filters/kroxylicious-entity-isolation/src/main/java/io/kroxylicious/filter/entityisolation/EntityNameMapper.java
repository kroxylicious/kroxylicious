/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.entityisolation;

import io.kroxylicious.filter.entityisolation.EntityIsolation.ResourceType;

/**
 * Maps an unmapped kafka resource name into a mapped one, or vice versa.
 * <br/>
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
     * @throws UnacceptableEntityNameException generated name for the entity is unacceptable
     *
     */
    String map(MapperContext mapperContext,
               ResourceType resourceType,
               String unmappedResourceName)
            throws UnacceptableEntityNameException;

    /**
     * Maps a mapped resource name back into an unmapped one.
     *
     * @param mapperContext mapper context.
     * @param resourceType resource type.
     * @param mappedResourceName mapped resource name.
     * @return unmapped resource name
     * @throws UnacceptableEntityNameException generated name for the entity is unacceptable
     */
    String unmap(MapperContext mapperContext,
                 ResourceType resourceType,
                 String mappedResourceName)
            throws UnacceptableEntityNameException;

    /**
     * Tests whether the given mapped resource name belongs in this namespace.
     *
     * @param mapperContext mapper context.
     * @param resourceType resource type.
     * @param mappedResourceName mapped resource name.
     * @return true if the mapped resource name belongs in this namespace, false otherwise.
     * @throws UnacceptableEntityNameException generated name for the entity is unacceptable
     */
    boolean isInNamespace(MapperContext mapperContext,
                          ResourceType resourceType,
                          String mappedResourceName)
            throws UnacceptableEntityNameException;

    /**
     * Signals that the entity name that would be created by the mapper is somehow invalid.
     */
    class UnacceptableEntityNameException extends RuntimeException {
        public UnacceptableEntityNameException(String message) {
            super(message);
        }

        public UnacceptableEntityNameException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
