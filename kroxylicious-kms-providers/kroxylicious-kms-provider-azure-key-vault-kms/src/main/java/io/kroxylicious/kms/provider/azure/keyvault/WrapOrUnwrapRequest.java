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
 * Request body for Azure Key Vault wrap or unwrap operations.
 *
 * @param algorithm wrap/unwrap algorithm
 * @param value base64 urlencoded bytes to wrap or unwrap
 */
public record WrapOrUnwrapRequest(@JsonProperty("alg") String algorithm, String value) {
    /**
     * Validates the record components.
     * @throws NullPointerException if any component is null.
     */
    public WrapOrUnwrapRequest {
        Objects.requireNonNull(algorithm, "alg cannot be null");
        Objects.requireNonNull(value, "value cannot be null");
    }

    /**
     * Creates a request, base64 url-encoding the given bytes.
     *
     * @param algorithm wrap/unwrap algorithm.
     * @param value the bytes to wrap or unwrap.
     * @return the request.
     */
    public static WrapOrUnwrapRequest from(String algorithm, byte[] value) {
        String valueB64 = Base64.getUrlEncoder().encodeToString(value);
        return new WrapOrUnwrapRequest(algorithm, valueB64);
    }
}
