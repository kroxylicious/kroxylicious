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
 * Decrypt request from Fortanix DSM REST API.
 *
 * @param kid UUID of the secure object
 * @param request request
 */
public record DecryptRequest(@JsonProperty(value = "kid", required = true) @NonNull String kid,
                             @JsonProperty(value = "request", required = true) @NonNull DecryptRequest.Request request) {
    /**
     *
     * @param alg A cryptographic algorithm (AES etc)
     * @param mode cipher mode
     * @param iv The initialization vector to use,
     * @param cipher Ciphertext bytes to be decrypted.
     */
    @SuppressWarnings("java:S6218") // we don't need EncryptResponse equality
    public record Request(
                          @JsonProperty(value = "alg", required = true) String alg,
                          @JsonProperty(value = "mode", required = true) String mode,
                          @JsonProperty(value = "iv", required = true) byte[] iv,
                          @JsonProperty(value = "cipher") @NonNull byte[] cipher) {
        public Request {
            Objects.requireNonNull(alg);
            Objects.requireNonNull(mode);
            Objects.requireNonNull(iv);
        }
    }

    @NonNull
    public static DecryptRequest createUnwrapRequest(@NonNull String kid, byte[] iv, byte[] plaintext) {
        return new DecryptRequest(kid, new DecryptRequest.Request(EncryptRequest.AES, EncryptRequest.BATCH_ENCRYPT_CIPHER_MODE, iv, plaintext));
    }

}