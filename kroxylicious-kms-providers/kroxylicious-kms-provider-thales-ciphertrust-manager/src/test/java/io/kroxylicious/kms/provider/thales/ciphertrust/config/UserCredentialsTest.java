/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.thales.ciphertrust.config;

import java.io.IOException;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;

import io.kroxylicious.proxy.config.secret.InlinePassword;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class UserCredentialsTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void successfulParse() throws IOException {
        String json = """
                {
                    "username": "testuser",
                    "password": { "password": "testpass" }
                }
                """;
        UserCredentials credentials = MAPPER.reader().readValue(json, UserCredentials.class);
        assertThat(credentials.username()).isEqualTo("testuser");
        assertThat(credentials.password()).isInstanceOf(InlinePassword.class);
        assertThat(credentials.password().getProvidedPassword()).isEqualTo("testpass");
    }

    @Test
    void usernameRequired() {
        assertThatThrownBy(() -> {
            String json = """
                    {
                        "password": { "password": "testpass" }
                    }
                    """;
            MAPPER.reader().readValue(json, UserCredentials.class);
        }).isInstanceOf(MismatchedInputException.class).hasMessageContaining("username");
    }

    @Test
    void passwordRequired() {
        assertThatThrownBy(() -> {
            String json = """
                    {
                        "username": "testuser"
                    }
                    """;
            MAPPER.reader().readValue(json, UserCredentials.class);
        }).isInstanceOf(MismatchedInputException.class).hasMessageContaining("password");
    }

}
