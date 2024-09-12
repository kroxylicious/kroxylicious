/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.service;

import java.util.concurrent.CompletionStage;

import javax.crypto.SecretKey;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Abstracts the KMS operations needed for Envelope Encryption
 * @param <K> The type of Key Encryption Key id.
 * @param <E> The type of encrypted Data Encryption Key.
 */
public interface Kms<K, E> {

    /**
     * Asynchronously generates a new Data Encryption Key (DEK) and returns it together with the same DEK wrapped by the Key Encryption Key (KEK) given
     * by the {@code kekRef},
     * The returned encrypted DEK can later be decrypted with {@link #decryptEdek(Object)}. It is expected that
     * the returned EDEK contains everything required for decryption including an immutable reference to the KEK
     * @param kekRef The key encryption key used to encrypt the generated data encryption key.
     * @return A completion stage for the wrapped data encryption key.
     * @throws UnknownKeyException If the kek was not known to this KMS.
     * @throws InvalidKeyUsageException If the given kek was not intended for key wrapping.
     * @throws KmsException For other exceptions.
     */
    @NonNull
    CompletionStage<DekPair<E>> generateDekPair(
            @NonNull
            K kekRef
    );

    /**
     * <p>Asynchronously decrypts a data encryption key that was {@linkplain #generateDekPair(Object) previously encrypted}.</p>
     *
     * <p><strong>Note:</strong> It is strongly recommended that the implementation of {@code SecretKey} returned in the {@code CompletionStage}
     * overrides {@link SecretKey#destroy()} to actually destroy the key material.
     * {@link DestroyableRawSecretKey} provides such an implementation for use by implementers.</p>
     *
     * @param edek The encrypted data encryption key.
     * @return A completion stage for the data encryption key
     * @throws UnknownKeyException If the edek was not encrypted by a KEK known to this KMS.
     * @throws InvalidKeyUsageException If the edek refers to a kek that was not intended for key wrapping.
     * @throws KmsException For other exceptions
     */
    @NonNull
    CompletionStage<SecretKey> decryptEdek(
            @NonNull
            E edek
    );

    /**
     * Get a serializer for encrypted DEKs.
     * It is required that {@code deserialize(serialize(edek)).equals(edek)}.
     *
     * @return a serializer for encrypted DEKs.
     */
    @NonNull
    Serde<E> edekSerde();

    /**
     * Asynchronously resolve an alias to a key id
     * @param alias The alias
     * @return A completion stage for the key id.
     * @throws UnknownAliasException If the alias does not resolve to a key in this KMS.
     * @throws KmsException For other exceptions.
     */
    @NonNull
    CompletionStage<K> resolveAlias(
            @NonNull
            String alias
    );
}
