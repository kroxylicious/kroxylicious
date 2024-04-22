/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.common;

/**
 * <p>Abstraction for identifying specific behaviours (implementations such as {@link io.kroxylicious.filter.encryption.dek.Aes#AES_256_GCM_128}
 * or {@link io.kroxylicious.filter.encryption.dek.ChaChaPoly#INSTANCE})
 * from a set of possible behaviours (interfaces such as {@link io.kroxylicious.filter.encryption.dek.CipherManager}) in a way that guarantees
 * backwards compatibility between releases of the filter.</p>
 *
 * <p>Each behaviour is {@linkplain #name() named} by an element from an {@code enum}.
 * This is typically used for identifying the behaviour within configuration.</p>
 *
 * <p>Each behaviour is also identified by a {@link #serializedId()}. The mapping between behaviour and id must be unique in both directions.</p>
 *
 * <p>A {@link Resolver} is usually responsible for finding the implementation corresponding to a given {@link #name()} or {@link #serializedId()}.</p>
 *
 * <h2>Example</h2>
 * <p>Users refer to a specific cipher via an element of the enum {@link io.kroxylicious.filter.encryption.config.CipherSpec} (this is the name).
 * When a particular {@link io.kroxylicious.filter.encryption.dek.CipherManager} implementation (such as {@link io.kroxylicious.filter.encryption.dek.Aes#AES_256_GCM_128})
 * is used to encrypt a plaintext we store it's {@link #serializedId()} in the record.
 * When we need to decrypt that a ciphertext we use the {@link #serializedId()} within the record to recover the implementation.</p>
 *
 * @param <E> The enum of the allowed names.
 */
public interface PersistedIdentifiable<E extends Enum<E>> {

    /**
     * @return The id to be used when serializing a reference to an implementation.
     */
    byte serializedId();

    /**
     * @return The name to be used to refer to the implementation.
     */
    E name();
}
