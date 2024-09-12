/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.aws.kms.model;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.umd.cs.findbugs.annotations.NonNull;

@JsonIgnoreProperties(ignoreUnknown = true)
@SuppressWarnings("java:S6218") // we don't need DecryptResponse equality
public record DecryptResponse(
        @JsonProperty(value = "KeyId") @NonNull
        String keyId,
        @JsonProperty(value = "Plaintext") @NonNull
        byte[] plaintext
) {

    public DecryptResponse {
        Objects.requireNonNull(keyId);
        Objects.requireNonNull(plaintext);
    }
}
