/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config.tls;

import com.fasterxml.jackson.annotation.JsonCreator;

/**
 * A password expressed directly in the model, in plain text.
 * <strong>Not recommended for production use.</strong>
 * @param password the password
 */
public record InlinePassword(String password) implements PasswordProvider {
    @JsonCreator
    public InlinePassword {}

    @Override
    public String getProvidedPassword() {
        return password;
    }

    @Override
    public String toString() {
        return "InlinePassword[" +
                "password=" + (password == null ? null : "*******") + ']';
    }

}
