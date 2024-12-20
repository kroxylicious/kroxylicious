/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.fortanix.dsm.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 *
 * @param kid
 * @param transientKey
 * @param value Security object stored as byte array (populated by export)
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record SecurityObjectResponse(@JsonProperty(value = "kid", required = false) String kid,
                                     @JsonProperty(value = "transient_key", required = false) String transientKey,
                                     @JsonProperty(value = "value", required = false) byte[] value) {
    public SecurityObjectResponse {
        if (kid == null && transientKey == null) {
            throw new NullPointerException();
        }
    }
}
