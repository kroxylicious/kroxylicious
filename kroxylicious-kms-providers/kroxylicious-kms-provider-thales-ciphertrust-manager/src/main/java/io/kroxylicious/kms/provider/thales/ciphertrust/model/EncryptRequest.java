/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.thales.ciphertrust.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Request model for CipherTrust Manager encryption operation.
 * Jackson automatically handles base64 encoding of byte[].
 *
 * @param id the key ID to use for encryption
 * @param plaintext the plaintext to encrypt (base64 encoded by Jackson)
 * @param type id type
 */
@SuppressWarnings("java:S6218") // no need for toString, equals, hashCode to go deep on the byte[]
public record EncryptRequest(
                             @JsonProperty("id") String id,
                             @SuppressWarnings("ArrayRecordComponent") @JsonProperty("plaintext") byte[] plaintext,
                             @JsonProperty("type") String type) {}
