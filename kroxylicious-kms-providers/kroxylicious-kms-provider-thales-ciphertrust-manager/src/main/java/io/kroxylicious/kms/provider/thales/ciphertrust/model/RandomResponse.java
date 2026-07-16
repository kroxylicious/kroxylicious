/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.thales.ciphertrust.model;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Response model for CipherTrust Manager random bytes generation.
 * Jackson automatically handles base64 decoding to byte[].
 *
 * @param bytes the random bytes (base64 decoded by Jackson)
 */
@SuppressWarnings("java:S6218") // no need for toString, equals, hashCode to go deep on the byte[]
@JsonIgnoreProperties(ignoreUnknown = true)
public record RandomResponse(
                             @SuppressWarnings("ArrayRecordComponent") @JsonProperty("bytes") byte[] bytes) {

    /**
     * Constructs a random response.
     */
    public RandomResponse {
        if (bytes == null) { throw new NullPointerException("bytes cannot be null"); }
    }
}
