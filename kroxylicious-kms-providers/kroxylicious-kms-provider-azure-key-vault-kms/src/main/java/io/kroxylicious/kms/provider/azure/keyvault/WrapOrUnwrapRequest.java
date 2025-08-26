/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.azure.keyvault;

import java.util.Base64;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * @param algorithm wrap/unwrap algorithm
 * @param value base64 urlencoded bytes to wrap or unwrap
 */
public record WrapOrUnwrapRequest(@JsonProperty("alg") String algorithm, String value) {
    public WrapOrUnwrapRequest {
        Objects.requireNonNull(algorithm, "alg cannot be null");
        Objects.requireNonNull(value, "value cannot be null");
    }

    public static WrapOrUnwrapRequest from(String algorithm, byte[] value) {
        String valueB64 = Base64.getUrlEncoder().encodeToString(value);
        return new WrapOrUnwrapRequest(algorithm, valueB64);
    }
}
