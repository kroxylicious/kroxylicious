/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.fortanix.dsm.model;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;

import static io.kroxylicious.kms.provider.fortanix.dsm.model.Constants.AES;
import static io.kroxylicious.kms.provider.fortanix.dsm.model.Constants.BATCH_ENCRYPT_CIPHER_MODE;

/**
 * Decrypt request to Fortanix DSM REST API, {@code /crypto/v1/decrypt}.
 *
 * @param key UUID of the secure object
 * @param alg A cryptographic algorithm (AES etc)
 * @param mode cipher mode
 * @param iv The initialization vector to use,
 * @param cipher Ciphertext bytes to be decrypted.
 */
@SuppressWarnings("java:S6218") // we don't need DecryptRequest equality
public record DecryptRequest(@JsonProperty(value = "key", required = true) SecurityObjectDescriptor key,
                             @JsonProperty(value = "alg", required = true) String alg,
                             @JsonProperty(value = "mode", required = true) String mode,
                             @JsonProperty(value = "iv", required = true) byte[] iv,
                             @JsonProperty(value = "cipher") byte[] cipher) {
    /**
     * Decrypt request to Fortanix DSM REST API, {@code /crypto/v1/decrypt}.
     *
     * @param key UUID of the secure object
     * @param alg A cryptographic algorithm (AES etc)
     * @param mode cipher mode
     * @param iv The initialization vector to use,
     * @param cipher Ciphertext bytes to be decrypted.
     */
    public DecryptRequest {
        Objects.requireNonNull(key);
        Objects.requireNonNull(alg);
        Objects.requireNonNull(mode);
        Objects.requireNonNull(iv);
        Objects.requireNonNull(cipher);
        if (iv.length == 0) {
            throw new IllegalArgumentException("iv cannot be empty");
        }
        if (cipher.length == 0) {
            throw new IllegalArgumentException("cipher cannot be empty");
        }

    }

    /**
     * Factory method to create an unwrap request.
     *
     * @param kid UUID of the secure object
     * @param iv The initialization vector to use,
     * @param cipher Ciphertext bytes to be decrypted.
     * @return decrypt request
     */
    public static DecryptRequest createUnwrapRequest(String kid,
                                                     byte[] iv,
                                                     byte[] cipher) {
        return new DecryptRequest(new SecurityObjectDescriptor(kid, null, null), AES, BATCH_ENCRYPT_CIPHER_MODE, iv, cipher);
    }

    @Override
    public String toString() {
        return "DecryptRequest{" +
                "key=" + key +
                ", alg='" + alg + '\'' +
                ", mode='" + mode + '\'' +
                ", iv='*********'" +
                ", cipher='*********'" +
                '}';
    }
}