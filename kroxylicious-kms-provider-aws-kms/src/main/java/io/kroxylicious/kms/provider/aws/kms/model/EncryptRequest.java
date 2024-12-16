/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.aws.kms.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import edu.umd.cs.findbugs.annotations.NonNull;

import java.util.Objects;

@SuppressWarnings("java:S6218") // we don't need DecryptRequest equality
public record EncryptRequest(@JsonProperty(value = "kid") @NonNull String keyId,
                             @JsonProperty(value = "request") @NonNull Request request) {
    public EncryptRequest {
        Objects.requireNonNull(keyId);
        Objects.requireNonNull(request);
    }

    public record Request(@JsonProperty(value = "key") @NonNull String keyId,
                          @JsonProperty(value = "alg") @NonNull String alg,
                          @JsonProperty(value = "plain") byte[] plain,
                          @JsonProperty(value = "iv", required = false) byte[] iv,
                          @JsonProperty(value = "ad", required = false) byte[] ad,
                          @JsonProperty(value = "tag_len", required = false) int tagLen,
                          @JsonProperty(value = "label", required = false) String label
                          ) {
    }
}
