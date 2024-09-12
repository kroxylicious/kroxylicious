/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.common;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Abstracts the lookup of a {@linkplain PersistedIdentifiable#name() name} or
 * {@linkplain PersistedIdentifiable#serializedId() serializedId} to
 * yield {@linkplain PersistedIdentifiable an implementation} instance.
 * @param <E> The enum of names
 * @param <T> The type of PersistedIdentifiable
 */
public interface Resolver<E extends Enum<E>, T> {
    /**
     * Look up the implementation instance corresponding to the given enum element name,
     * or throw some exception if this name is not known to the resolver.
     * @param element The enum element name.
     * @return the implementation instance.
     */
    @NonNull
    T fromName(@NonNull
    E element);

    /**
     * Look up the implementation instance corresponding to the given serialized id,
     * or throw some exception if this id is not known to the resolver.
     * @param id The serialized id.
     * @return the implementation instance.
     */
    @NonNull
    T fromSerializedId(byte id);

    /**
     * Look up the serialized id corresponding to the given implementation instance,
     * or throw some exception if this implementation instance is not known to the resolver.
     * @param impl The implementation instance.
     * @return the serialized id.
     */
    byte toSerializedId(@NonNull
    T impl);
}
