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
 * Encrypt request to the Fortanix DSM REST API.
 *
 * @param keyId key id that will be used to perform the encryption
 * @param request encryption request
 */
public record EncryptRequest(@JsonProperty(value = "kid") @NonNull String keyId,
                             @JsonProperty(value = "request") @NonNull Request request) {

    public static final String BATCH_ENCRYPT_CIPHER_MODE = "CBC";
    public static final String AES = "AES";

    public EncryptRequest {
        Objects.requireNonNull(keyId);
        Objects.requireNonNull(request);
    }

    @SuppressWarnings("java:S6218") // we don't need EncryptResponse equality
    public record Request(
                          @JsonProperty(value = "alg") @NonNull String alg,
                          @JsonProperty(value = "plain") byte[] plain,
                          @JsonProperty(value = "mode") String mode) {

    }

    @NonNull
    public static EncryptRequest createWrapRequest(@NonNull String kid, byte[] plaintext) {
        return new EncryptRequest(kid, new Request(AES, plaintext, BATCH_ENCRYPT_CIPHER_MODE));
    }
}
