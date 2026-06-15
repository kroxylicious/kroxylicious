/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.thales.ciphertrust.config;

import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.config.secret.InlinePassword;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ClientCredentialsTest {

    @Test
    void shouldConstructValidCredentials() {
        var credentials = new ClientCredentials("client-id", new InlinePassword("secret"));

        assertThat(credentials.clientId()).isEqualTo("client-id");
        assertThat(credentials.clientSecret()).isNotNull();
    }

    @Test
    void shouldRejectNullClientId() {
        assertThatThrownBy(() -> new ClientCredentials(null, new InlinePassword("secret")))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("clientId cannot be null");
    }

    @Test
    void shouldRejectNullClientSecret() {
        assertThatThrownBy(() -> new ClientCredentials("client-id", null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("clientSecret cannot be null");
    }

    @Test
    void shouldRejectEmptyClientId() {
        assertThatThrownBy(() -> new ClientCredentials("", new InlinePassword("secret")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("clientId cannot be empty");
    }
}
