/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.fortanix.dsm.model;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Encrypt request to the Fortanix DSM REST API, {@code /crypto/v1/encrypt}.
 *
 * @param key Uniquely identifies a persisted or transient secure object.
 * @param alg A cryptographic algorithm.
 * @param mode cipher mode
 * @param plain Plaintext bytes to be encrypted.
 */
@SuppressWarnings("java:S6218") // we don't need EncryptRequest equality
public record EncryptRequest(@JsonProperty(value = "key", required = true) SecurityObjectDescriptor key,
                             @JsonProperty(value = "alg", required = true) String alg,
                             @JsonProperty(value = "mode", required = true) String mode,
                             @JsonProperty(value = "plain", required = true) byte[] plain) {

    public static final String BATCH_ENCRYPT_CIPHER_MODE = "CBC";
    public static final String AES = "AES";

    public EncryptRequest {
        Objects.requireNonNull(key);
        Objects.requireNonNull(alg);
        Objects.requireNonNull(plain);
        Objects.requireNonNull(mode);
    }

    @NonNull
    public static EncryptRequest createWrapRequest(@NonNull String kid, byte[] plaintext) {
        return new EncryptRequest(new SecurityObjectDescriptor(kid, null, null), AES, BATCH_ENCRYPT_CIPHER_MODE, plaintext);
    }

    @Override
    public String toString() {
        return "EncryptRequest{" +
                "key=" + key +
                ", alg='" + alg + '\'' +
                ", mode='" + mode + '\'' +
                ", plain='*********'" +
                '}';
    }
}
