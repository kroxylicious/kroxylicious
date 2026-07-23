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

    @SuppressWarnings("java:S6218") // no need for toString, equals, hashCode to go deep on the byte[]
    @JsonIgnoreProperties(ignoreUnknown = true)
    record DecryptData(@SuppressWarnings("ArrayRecordComponent") byte[] plaintext) { // byte[] retained: transient Jackson DTO; plaintext key material must stay zeroable
        DecryptData {
            Objects.requireNonNull(plaintext);
        }
    }

    @SuppressWarnings("java:S6218") // no need for toString, equals, hashCode to go deep on the byte[]
    @JsonIgnoreProperties(ignoreUnknown = true)
    record DataKeyData(@SuppressWarnings("ArrayRecordComponent") byte[] plaintext, String ciphertext) { // byte[] retained: transient Jackson DTO; plaintext key material must stay zeroable
        DataKeyData {
            Objects.requireNonNull(plaintext);
            Objects.requireNonNull(ciphertext);
        }
    }
}
