/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.entityisolation;

import io.kroxylicious.filter.entityisolation.EntityIsolation.EntityType;

/**
 * Maps from a downstream kafka resource name to an upstream name (or vice-versa).
 * <br/>
 * The {@link #unmap(MapperContext, EntityType, String)} function must be the reciprocal of
 * the {@link #map(MapperContext, EntityType, String)}.
 */
public interface EntityNameMapper {

    /**
     * Validates that the context provided is acceptable for use with this mapper.
     * The mapper implementation us guaranteed that this method is at least once
     * before any mapping operations are performed with the same context.
     *
     * @param mapperContext mapper context.
     * @throws EntityMapperException the provided context is unsuitable.
     */
    void validateContext(MapperContext mapperContext) throws EntityMapperException;

    /**
     * Maps a downstream kafka resource name to an upstream name.
     *
     * @param mapperContext mapper context.
     * @param resourceType resource type.
     * @param downstreamResourceName downstream resource name.
     * @return upstream resource name
     * @throws EntityMapperException the mapped resource name violates one or more system constraints.
     */
    String map(MapperContext mapperContext,
               EntityType resourceType,
               String downstreamResourceName)
            throws EntityMapperException;

    /**
     *  Maps an upstream kafka resource name to a downstream name.
     *
     * @param mapperContext mapper context.
     * @param resourceType resource type.
     * @param upstreamResourceName upstream resource name.
     * @return downstream resource name
     * @throws EntityMapperException the mapped resource name violates one or more system constraints.
     */
    String unmap(MapperContext mapperContext,
                 EntityType resourceType,
                 String upstreamResourceName)
            throws EntityMapperException;

    /**
     * Tests whether the given upstreams resource name belongs to this context.
     *
     * @param mapperContext mapper context.
     * @param resourceType resource type.
     * @param upstreamResourceName upstream resource name.
     * @return true if the mapped resource name belongs to this context, false otherwise.
     */
    boolean isOwnedByContext(MapperContext mapperContext,
                             EntityType resourceType,
                             String upstreamResourceName);

    /**
     * Signals that the entity name that would be created by the mapper is somehow invalid.
     */
    class EntityMapperException extends RuntimeException {
        public EntityMapperException(String message) {
            super(message);
        }

        public EntityMapperException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
