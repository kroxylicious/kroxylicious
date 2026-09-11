/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.kms.vault.model;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * A request to create a Vault policy.
 *
 * @param policy the policy document, in HCL format.
 * @see <a href="https://developer.hashicorp.com/vault/api-docs/system/policy#create-update-policy">Vault API Create/Update Policy</a>
 */
public record CreatePolicyRequest(String policy) {

    /**
     * Creates a CreatePolicyRequest from an input stream.
     *
     * @param is stream containing the policy document, in HCL format.
     * @return the CreatePolicyRequest.
     */
    public static CreatePolicyRequest fromInputStream(InputStream is) {
        Objects.requireNonNull(is);
        try {
            return new CreatePolicyRequest(new String(is.readAllBytes(), StandardCharsets.UTF_8));
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to create a CreatePolicy", e);
        }
    }
}
