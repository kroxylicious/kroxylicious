/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.fortanix.dsm.model;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Fortanix key info <a href="https://support.fortanix.com/apidocs/lookup-a-security-object">request</a> looking up by
 * name.
 *
 * @param name key alias
 */
public record InfoRequest(@JsonProperty(value = "name") @NonNull String name) {

    public InfoRequest {
        Objects.requireNonNull(name);
    }
}
