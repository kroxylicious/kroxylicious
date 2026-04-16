/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.plugin;

/**
 * Converts a typed resource to bytes for snapshot generation.
 *
 * @param <T> the type of resource to serialise
 */
public interface ResourceSerializer<T> {

    /**
     * Serialises the given resource to bytes.
     *
     * @param resource the resource to serialise
     * @return the serialised bytes
     */
    byte[] serialize(T resource);
}
