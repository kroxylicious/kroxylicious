/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.entityisolation;

import io.kroxylicious.filter.entityisolation.EntityIsolation.ResourceType;

/**
 * Maps from a downstream kafka resource name to an upstream name (or vice-versa).
 * <br/>
 * The {@link #unmap(MapperContext, ResourceType, String)} function must be the reciprocal of
 * the {@link #map(MapperContext, ResourceType, String)}.
 */
public interface EntityNameMapper {
    /**
     * Maps a downstream kafka resource name to an upstream name.
     *
     * @param mapperContext mapper context.
     * @param resourceType resource type.
     * @param downstreamResourceName downstream resource name.
     * @return upstream resource name
     * @throws UnacceptableEntityNameException generated name for the entity is unacceptable
     *
     */
    String map(MapperContext mapperContext,
               ResourceType resourceType,
               String downstreamResourceName)
            throws UnacceptableEntityNameException;

    /**
     *  Maps an upstream kafka resource name to a downstream name.
     *
     * @param mapperContext mapper context.
     * @param resourceType resource type.
     * @param upstreamResourceName upstream resource name.
     * @return downstream resource name
     * @throws UnacceptableEntityNameException generated name for the entity is unacceptable
     */
    String unmap(MapperContext mapperContext,
                 ResourceType resourceType,
                 String upstreamResourceName)
            throws UnacceptableEntityNameException;

    /**
     * Tests whether the given upstreams resource name belongs in this namespace.
     *
     * @param mapperContext mapper context.
     * @param resourceType resource type.
     * @param upstreamResourceName upstream resource name.
     * @return true if the mapped resource name belongs in this namespace, false otherwise.
     * @throws UnacceptableEntityNameException generated name for the entity is unacceptable
     */
    boolean isInNamespace(MapperContext mapperContext,
                          ResourceType resourceType,
                          String upstreamResourceName)
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
