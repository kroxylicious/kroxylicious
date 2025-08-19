/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.fortanix.dsm.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Security object response from the Fortanix DSM REST API.
 *
 * @param kid kid
 * @param transientKey transient key name
 * @param value key material (populated by the export endpoint only)
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@SuppressWarnings("java:S6218") // we don't need SecurityObjectResponse equality
public record SecurityObjectResponse(@JsonProperty(value = "kid", required = false) @Nullable String kid,
                                     @JsonProperty(value = "transient_key", required = false) @Nullable String transientKey,
                                     @JsonProperty(value = "value", required = false) @Nullable byte[] value) {
    /**
     * Security object response from the Fortanix DSM REST API.
     *
     * @param kid kid
     * @param transientKey transient key name
     * @param value key material (populated by the export endpoint only)
     */
    public SecurityObjectResponse {
        if (kid == null && transientKey == null) {
            throw new NullPointerException("Requires a key identifier");
        }
        if (value != null && value.length == 0) {
            throw new IllegalArgumentException("If value is present, value is required");
        }
    }

    @Override
    public String toString() {
        return "SecurityObjectResponse{" +
                "kid='" + kid + '\'' +
                ", transientKey='" + transientKey + '\'' +
                ", value=" + (value == null ? "<not present>" : "*********") +
                '}';
    }
}
