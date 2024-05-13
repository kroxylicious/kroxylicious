/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.aws.kms.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@SuppressWarnings("java:S6218")
public record GenerateDataKeyResponse(@JsonProperty(value = "CiphertextBlob") byte[] ciphertextBlob,
                                      @JsonProperty(value = "KeyId") String keyId,
                                      @JsonProperty(value = "Plaintext") byte[] plaintext) {

}
