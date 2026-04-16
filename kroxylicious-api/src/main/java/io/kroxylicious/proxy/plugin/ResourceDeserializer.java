/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.plugin;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Converts bytes to a typed resource for loading from a snapshot.
 *
 * @param <T> the type of resource to deserialise
 */
public interface ResourceDeserializer<T> {

    /**
     * Deserialises the given bytes into a typed resource.
     *
     * @param data the raw resource bytes
     * @param password the resource password, or {@code null} if no password is configured
     * @return the deserialised resource
     */
    T deserialize(
                  byte[] data,
                  @Nullable char[] password);
}
