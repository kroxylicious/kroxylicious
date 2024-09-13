/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config.secret;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A password expressed directly in the model, in plain text.
 * <strong>Not recommended for production use.</strong>
 * @param password the password
 */
public record InlinePassword(
        @JsonProperty(required = true)
        String password
) implements PasswordProvider {

    public InlinePassword {
        Objects.requireNonNull(password);
    }

    @Override
    public String getProvidedPassword() {
        return password;
    }

    @SuppressWarnings("java:S2068") // we aren't hard-coding a password here
    @Override
    public String toString() {
        return "InlinePassword[password=*******]";
    }

}
