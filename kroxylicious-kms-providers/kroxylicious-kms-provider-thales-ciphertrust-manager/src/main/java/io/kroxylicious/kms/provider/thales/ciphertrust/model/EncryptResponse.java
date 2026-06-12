/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.thales.ciphertrust.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Response model for CipherTrust Manager encryption operation.
 * Jackson automatically handles base64 decoding to byte[].
 *
 * @param ciphertext the ciphertext (base64 decoded by Jackson)
 * @param tag the authentication tag (base64 decoded by Jackson)
 * @param id the key ID used for encryption
 * @param version the key version
 * @param mode the encryption mode (e.g., "gcm")
 * @param iv the initialization vector (base64 decoded by Jackson)
 */
@SuppressWarnings("java:S6218") // no need for toString, equals, hashCode to go deep on the byte[]
@JsonIgnoreProperties(ignoreUnknown = true)
public record EncryptResponse(
                              @SuppressWarnings("ArrayRecordComponent") @JsonProperty("ciphertext") byte[] ciphertext,
                              @SuppressWarnings("ArrayRecordComponent") @JsonProperty("tag") byte[] tag,
                              @JsonProperty("id") String id,
                              @JsonProperty("version") int version,
                              @JsonProperty("mode") String mode,
                              @SuppressWarnings("ArrayRecordComponent") @JsonProperty("iv") byte[] iv) {}
