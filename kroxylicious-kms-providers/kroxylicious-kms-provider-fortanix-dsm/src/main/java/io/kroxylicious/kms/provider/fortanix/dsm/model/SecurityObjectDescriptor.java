/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.fortanix.dsm.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Fortanix key descriptor capable of expressing a security object by kid, name or transient key id.
 *
 * @param kid kid
 * @param name key name
 * @param transientKey transient key name
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record SecurityObjectDescriptor(
                                       @JsonProperty("kid") @Nullable String kid,
                                       @JsonProperty("name") @Nullable String name,
                                       @JsonProperty("transient_key") @Nullable String transientKey) {
    /**
     * Fortanix key descriptor capable of expressing a security object by kid, name or transient key id.
     *
     * @param kid kid
     * @param name key name
     * @param transientKey transient key name
     */
    public SecurityObjectDescriptor {
        if (kid == null && name == null && transientKey == null) {
            throw new NullPointerException();
        }
    }
}
