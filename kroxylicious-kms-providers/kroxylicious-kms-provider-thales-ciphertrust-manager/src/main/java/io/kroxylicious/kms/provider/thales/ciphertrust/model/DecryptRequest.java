/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.thales.ciphertrust.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Request model for CipherTrust Manager decryption operation.
 * Jackson automatically handles base64 encoding of byte[].
 *
 * @param ciphertext the ciphertext to decrypt (base64 encoded by Jackson)
 * @param tag the authentication tag (base64 encoded by Jackson)
 * @param id the key ID to use for decryption
 * @param version the key version
 * @param mode the encryption mode (e.g., "gcm")
 * @param iv the initialization vector (base64 encoded by Jackson)
 */
@SuppressWarnings("java:S6218") // no need for toString, equals, hashCode to go deep on the byte[]
public record DecryptRequest(
                             @SuppressWarnings("ArrayRecordComponent") @JsonProperty("ciphertext") byte[] ciphertext,
                             @SuppressWarnings("ArrayRecordComponent") @JsonProperty("tag") byte[] tag,
                             @JsonProperty("id") String id,
                             @JsonProperty("version") int version,
                             @JsonProperty("mode") String mode,
                             @SuppressWarnings("ArrayRecordComponent") @JsonProperty("iv") byte[] iv) {}
