/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.aws.kms.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import edu.umd.cs.findbugs.annotations.NonNull;

import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
public record EncryptResponse(@JsonProperty(value = "kid") @NonNull String kid,
                              @JsonProperty(value = "cipher") @NonNull byte[] cipher,
                              @JsonProperty(value = "iv") @NonNull byte[] iv,
                              @JsonProperty(value = "tag") @NonNull byte[] tag
) {

    public EncryptResponse {
        Objects.requireNonNull(kid);
        Objects.requireNonNull(cipher);
        Objects.requireNonNull(iv);
        Objects.requireNonNull(tag);
    }
}
