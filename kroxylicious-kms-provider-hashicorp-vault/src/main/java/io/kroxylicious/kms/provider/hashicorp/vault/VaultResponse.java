/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.hashicorp.vault;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public record VaultResponse<D>(D data) {

    public VaultResponse {
        Objects.requireNonNull(data);
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record ReadKeyData(String name, @JsonProperty("latest_version") int latestVersion) {
        public ReadKeyData {
            Objects.requireNonNull(name);
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    record DecryptData(String plaintext) {
        DecryptData {
            Objects.requireNonNull(plaintext);
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    record DataKeyData(String plaintext, String ciphertext) {
        DataKeyData {
            Objects.requireNonNull(plaintext);
            Objects.requireNonNull(ciphertext);
        }
    }
}
