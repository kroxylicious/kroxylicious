/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.thales.ciphertrust.config;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.kroxylicious.proxy.config.secret.PasswordProvider;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * User credentials for Thales CipherTrust Manager authentication.
 *
 * @param username the username
 * @param password the password provider
 * @param domain   optional CipherTrust domain; when set, the token request targets that domain
 */
public record UserCredentials(
                              @JsonProperty(required = true) String username,
                              @JsonProperty(required = true) PasswordProvider password,
                              @Nullable String domain) {
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
        return "UserCredentials{username='" + username + "', password='***', domain='" + domain + "'}";
    }
}
