/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.thales.ciphertrust.config;

import java.io.IOException;
import java.net.URI;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import com.fasterxml.jackson.databind.exc.ValueInstantiationException;

import io.kroxylicious.kms.service.KmsException;
import io.kroxylicious.proxy.config.tls.InsecureTls;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ConfigParseTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void endpointUrlAndUserCredentials() throws IOException {
        String json = """
                {
                    "endpointUrl": "https://ctm.example.com",
                    "userCredentials": {
                        "username": "testuser",
                        "password": { "password": "testpass" }
                    }
                }
                """;
        Config config = readConfig(json);
        assertThat(config.endpointUrl()).isEqualTo(URI.create("https://ctm.example.com"));
        assertThat(config.userCredentials()).isNotNull();
        assertThat(config.userCredentials().username()).isEqualTo("testuser");
        assertThat(config.userCredentials().password().getProvidedPassword()).isEqualTo("testpass");
        assertThat(config.clientCredentials()).isNull();
    }

    @Test
    void endpointUrlRequired() {
        assertThatThrownBy(() -> {
            String json = """
                    {
                        "userCredentials": {
                            "username": "testuser",
                            "password": { "password": "testpass" }
                        }
                    }
                    """;
            readConfig(json);
        }).isInstanceOf(MismatchedInputException.class).hasMessageContaining("endpointUrl");
    }

    @Test
    void endpointUrlShouldNotBeNull() {
        assertThatThrownBy(() -> {
            String json = """
                    {
                        "endpointUrl": null,
                        "userCredentials": {
                            "username": "testuser",
                            "password": { "password": "testpass" }
                        }
                    }
                    """;
            readConfig(json);
        }).isInstanceOf(ValueInstantiationException.class).cause().isInstanceOf(NullPointerException.class);
    }

    @Test
    void credentialsRequired() {
        assertThatThrownBy(() -> {
            String json = """
                    {
                        "endpointUrl": "https://ctm.example.com"
                    }
                    """;
            readConfig(json);
        }).isInstanceOf(ValueInstantiationException.class)
                .cause()
                .isInstanceOf(KmsException.class)
                .hasMessageContaining("Either userCredentials or clientCredentials must be specified");
    }

    @Test
    void emptyTls() throws Exception {
        String json = """
                {
                    "endpointUrl": "https://ctm.example.com",
                    "userCredentials": {
                        "username": "testuser",
                        "password": { "password": "testpass" }
                    },
                    "tls": {}
                }
                """;
        Config config = readConfig(json);
        assertThat(config.tls()).isNotNull();
        assertThat(config.tls().trust()).isNull();
    }

    @Test
    void missingTls() throws Exception {
        String json = """
                {
                    "endpointUrl": "https://ctm.example.com",
                    "userCredentials": {
                        "username": "testuser",
                        "password": { "password": "testpass" }
                    }
                }
                """;
        Config config = readConfig(json);
        assertThat(config.tls()).isNull();
    }

    @Test
    void testTlsTrust() throws Exception {
        String json = """
                {
                    "endpointUrl": "https://ctm.example.com",
                    "userCredentials": {
                        "username": "testuser",
                        "password": { "password": "testpass" }
                    },
                    "tls": {
                        "trust": {
                            "insecure": true
                        }
                    }
                }
                """;
        Config config = readConfig(json);
        assertThat(config.tls()).isNotNull();
        assertThat(config.tls().trust()).isInstanceOf(InsecureTls.class);
        assertThat(((InsecureTls) config.tls().trust()).insecure()).isTrue();
    }

    @Test
    void cannotSpecifyBothCredentialTypes() {
        assertThatThrownBy(() -> {
            String json = """
                    {
                        "endpointUrl": "https://ctm.example.com",
                        "userCredentials": {
                            "username": "testuser",
                            "password": { "password": "testpass" }
                        },
                        "clientCredentials": {
                            "clientId": "client-id",
                            "clientSecret": { "password": "secret" }
                        }
                    }
                    """;
            readConfig(json);
        }).isInstanceOf(ValueInstantiationException.class)
                .cause()
                .isInstanceOf(KmsException.class)
                .hasMessageContaining("Cannot specify both userCredentials and clientCredentials");
    }

    private Config readConfig(String json) throws IOException {
        return MAPPER.reader().readValue(json, Config.class);
    }

}
