/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.hashicorp.vault;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Envelope of a response from the <a href="https://developer.hashicorp.com/vault/api-docs">Vault API</a>.
 *
 * @param data the payload of the response.
 * @param <D> the type of the response payload.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record VaultResponse<D>(D data) {

    /**
     * Validates the record components.
     */
    public VaultResponse {
        Objects.requireNonNull(data);
    }

    /**
     * Payload of a response from the Vault
     * <a href="https://developer.hashicorp.com/vault/api-docs/secret/transit#read-key">read key</a> API.
     *
     * @param name the name of the key.
     * @param latestVersion the latest (current) version of the key. Not currently listed on
     *                      HashiCorp's read-key API documentation, but observed present and
     *                      incrementing on rotation in every Vault release line from 1.2 through
     *                      2.0, and on every key type creatable on CE — including hmac, where the
     *                      {@code keys} map is absent but this field is not.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record ReadKeyData(String name,
                              @JsonProperty(value = "latest_version", required = true) int latestVersion) {
        /**
         * Validates the record components.
         */
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
