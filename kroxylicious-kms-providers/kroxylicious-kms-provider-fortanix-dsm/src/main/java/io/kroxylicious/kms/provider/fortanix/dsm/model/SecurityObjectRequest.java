/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.fortanix.dsm.model;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Security object request to the Fortanix DSM REST API.
 *
 * @param name Name of the security object.
 * @param keySize Key size of the security object in bits.
 * @param objType Type of security object (AES, RSA etc.)
 * @param transientSo If set to true, the security object will cease to exist after session ends.
 * @param keyOps Array of key operations (EXPORT, ENCRYPT, etc.)
 * @param customMetadata User managed field for adding custom metadata to the security object.
 * @see <a href="https://support.fortanix.com/apidocs/generate-a-new-security-object">generate-a-new-security-object</a>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record SecurityObjectRequest(@JsonProperty("name") @Nullable String name,
                                    @JsonProperty("key_size") int keySize,
                                    @JsonProperty(value = "obj_type", required = true) String objType,
                                    @JsonProperty("transient") boolean transientSo,
                                    @JsonProperty("key_ops") @Nullable List<String> keyOps,
                                    @JsonProperty("custom_metadata") @Nullable Map<String, Object> customMetadata) {
    /**
     * Security object request to the Fortanix DSM REST API.
     *
     * @param name Name of the security object.
     * @param keySize Key size of the security object in bits.
     * @param objType Type of security object (AES, RSA etc.)
     * @param transientSo If set to true, the security object will cease to exist after session ends.
     * @param keyOps Array of key operations (EXPORT, ENCRYPT, etc.)
     * @param customMetadata User managed field for adding custom metadata to the security object.
     * @see <a href="https://support.fortanix.com/apidocs/generate-a-new-security-object">generate-a-new-security-object</a>
     */
    public SecurityObjectRequest {
        Objects.requireNonNull(objType);
    }
}
