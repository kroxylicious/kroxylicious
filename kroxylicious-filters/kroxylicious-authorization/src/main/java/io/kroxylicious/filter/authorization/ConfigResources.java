/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.authorization;

import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Stream;

import org.apache.kafka.common.config.ConfigResource;

/**
 * Centralize handling of ConfigResources to ensure we have a tested facility for mapping all known ConfigResource types.
 */
public class ConfigResources {

    /**
     * Prevent construction of utility class
     */
    private ConfigResources() {
        // Prevent construction
    }

    /**
     * Filters a stream, retaining only the elements which refer to config resources of the given type.
     * @param stream The stream to filter.
     * @param idByteFunction A function returning the config resource type id of an element of the stream.
     * @param type The config resource type to retain.
     * @return The filtered stream.
     * @param <T> The type of the stream elements.
     */
    public static <T> Stream<T> filter(Stream<T> stream, Function<T, Byte> idByteFunction, ConfigResource.Type type) {
        Objects.requireNonNull(stream);
        Objects.requireNonNull(idByteFunction);
        Objects.requireNonNull(type);
        return stream.filter(t -> {
            Byte configTypeIdByte = idByteFunction.apply(t);
            return type == ConfigResource.Type.forId(configTypeIdByte);
        });
    }
}
