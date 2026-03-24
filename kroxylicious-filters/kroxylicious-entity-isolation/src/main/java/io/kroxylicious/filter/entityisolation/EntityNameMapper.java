/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.entityisolation;

import io.kroxylicious.filter.entityisolation.EntityIsolation.EntityType;

/**
 * Maps from a downstream kafka entity name to an upstream name (or vice-versa).
 * <br/>
 * The {@link #unmap(MapperContext, EntityType, String)} function must be the reciprocal of
 * the {@link #map(MapperContext, EntityType, String)}.
 */
public interface EntityNameMapper {

    /**
     * Maps a downstream kafka entity name to an upstream name.
     *
     * @param mapperContext mapper context.
     * @param entityType entity type.
     * @param downstreamEntityName downstream entity name.
     * @return upstream entity name
     * @throws EntityMapperException the mapped entity name violates one or more system constraints.
     */
    String map(MapperContext mapperContext,
               EntityType entityType,
               String downstreamEntityName)
            throws EntityMapperException;

    /**
     *  Maps an upstream kafka entity name to a downstream name.
     *
     * @param mapperContext mapper context.
     * @param entityType entity type.
     * @param upstreamEntityName upstream entity name.
     * @return downstream entity name
     * @throws EntityMapperException the mapped entity name violates one or more system constraints.
     */
    String unmap(MapperContext mapperContext,
                 EntityType entityType,
                 String upstreamEntityName)
            throws EntityMapperException;

    /**
     * Tests whether the given upstreams entity name belongs to this context.
     *
     * @param mapperContext mapper context.
     * @param entityType entity type.
     * @param upstreamEntityName upstream entity name.
     * @return true if the mapped entity name belongs to this context, false otherwise.
     */
    boolean isOwnedByContext(MapperContext mapperContext,
                             EntityType entityType,
                             String upstreamEntityName);

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
