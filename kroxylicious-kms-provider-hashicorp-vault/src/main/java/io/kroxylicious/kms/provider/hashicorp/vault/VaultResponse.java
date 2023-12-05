/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.hashicorp.vault;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
record VaultResponse<D>(D data) {

    @JsonIgnoreProperties(ignoreUnknown = true)
    record ReadKeyData(String name, @JsonProperty("latest_version") int latestVersion) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    record DecryptData(String plaintext) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    record DataKeyData(String plaintext, String ciphertext) {}
}
