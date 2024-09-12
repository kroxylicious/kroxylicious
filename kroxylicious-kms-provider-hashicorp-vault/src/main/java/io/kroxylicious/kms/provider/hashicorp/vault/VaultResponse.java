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
record VaultResponse<D>(D data) {

    VaultResponse {
        Objects.requireNonNull(data);
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    record ReadKeyData(String name, @JsonProperty("latest_version")
    int latestVersion) {
        ReadKeyData {
            Objects.requireNonNull(name);
        }
    }

    @SuppressWarnings("java:S6218") // no need for toString, equals, hashCode to go deep on the byte[]
    @JsonIgnoreProperties(ignoreUnknown = true)
    record DecryptData(byte[] plaintext) {
        DecryptData {
            Objects.requireNonNull(plaintext);
        }
    }

    @SuppressWarnings("java:S6218") // no need for toString, equals, hashCode to go deep on the byte[]
    @JsonIgnoreProperties(ignoreUnknown = true)
    record DataKeyData(byte[] plaintext, String ciphertext) {
        DataKeyData {
            Objects.requireNonNull(plaintext);
            Objects.requireNonNull(ciphertext);
        }
    }
}
