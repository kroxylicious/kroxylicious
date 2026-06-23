/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.thales.ciphertrust.config;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.kroxylicious.proxy.config.secret.PasswordProvider;

/**
 * User credentials for Thales CipherTrust Manager authentication.
 *
 * @param username the username
 * @param password the password provider
 */
public record UserCredentials(
                              @JsonProperty(required = true) String username,
                              @JsonProperty(required = true) PasswordProvider password) {
    /**
     * Constructs user credentials.
     */
    public UserCredentials {
        Objects.requireNonNull(username, "username cannot be null");
        Objects.requireNonNull(password, "password cannot be null");
        if (username.isEmpty()) {
            throw new IllegalArgumentException("username cannot be empty");
        }
    }

    @Override
    @SuppressWarnings("java:S2068") // The masked password is a not a password
    public String toString() {
        return "UserCredentials{username='" + username + "', password='***'}";
    }
}
