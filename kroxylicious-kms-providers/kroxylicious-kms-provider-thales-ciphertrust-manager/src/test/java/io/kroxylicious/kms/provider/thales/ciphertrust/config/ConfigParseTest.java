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

import io.kroxylicious.proxy.config.tls.InsecureTls;
import io.kroxylicious.proxy.config.tls.Tls;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.InstanceOfAssertFactories.type;

class ConfigParseTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void endpointUrlAndUserCredentials() throws IOException {
        // Given
        String json = """
                {
                    "endpointUrl": "https://ctm.example.com",
                    "userCredentials": {
                        "username": "testuser",
                        "password": { "password": "testpass" }
                    }
                }
                """;

        // When
        Config config = readConfig(json);

        // Then
        assertThat(config.endpointUrl()).isEqualTo(URI.create("https://ctm.example.com"));
        assertThat(config.userCredentials()).isNotNull();
        assertThat(config.userCredentials().username()).isEqualTo("testuser");
        assertThat(config.userCredentials().password().getProvidedPassword()).isEqualTo("testpass");
    }

    @Test
    void endpointUrlAndUserCredentialsWithDomain() throws IOException {
        // Given
        String json = """
                {
                    "endpointUrl": "https://ctm.example.com",
                    "userCredentials": {
                        "username": "testuser",
                        "password": { "password": "testpass" },
                        "domain": "my-domain"
                    }
                }
                """;

        // When
        Config config = readConfig(json);

        // Then
        assertThat(config.userCredentials()).isNotNull();
        assertThat(config.userCredentials().domain()).isEqualTo("my-domain");
    }

    @Test
    void userCredentialsDomainIsOptional() throws IOException {
        // Given
        String json = """
                {
                    "endpointUrl": "https://ctm.example.com",
                    "userCredentials": {
                        "username": "testuser",
                        "password": { "password": "testpass" }
                    }
                }
                """;

        // When
        Config config = readConfig(json);

        // Then
        assertThat(config.userCredentials()).isNotNull();
        assertThat(config.userCredentials().domain()).isNull();
    }

    @Test
    void endpointUrlRequired() {
        // Given
        String json = """
                {
                    "userCredentials": {
                        "username": "testuser",
                        "password": { "password": "testpass" }
                    }
                }
                """;

        // When/Then
        assertThatThrownBy(() -> readConfig(json))
                .isInstanceOf(MismatchedInputException.class)
                .hasMessageContaining("endpointUrl");
    }

    @Test
    void endpointUrlShouldNotBeNull() {
        // Given
        String json = """
                {
                    "endpointUrl": null,
                    "userCredentials": {
                        "username": "testuser",
                        "password": { "password": "testpass" }
                    }
                }
                """;

        // When/Then
        assertThatThrownBy(() -> readConfig(json))
                .isInstanceOf(ValueInstantiationException.class)
                .cause()
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void credentialsRequired() {
        // Given
        String json = """
                {
                    "endpointUrl": "https://ctm.example.com"
                }
                """;

        // When/Then
        assertThatThrownBy(() -> readConfig(json))
                .isInstanceOf(ValueInstantiationException.class)
                .hasMessageContaining("Must configure either userCredentials or clientCredentials");
    }

    @Test
    void emptyTls() throws Exception {
        // Given
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

        // When
        Config config = readConfig(json);

        // Then
        assertThat(config.tls()).isNotNull();
        assertThat(config.tls().trust()).isNull();
    }

    @Test
    void missingTls() throws Exception {
        // Given
        String json = """
                {
                    "endpointUrl": "https://ctm.example.com",
                    "userCredentials": {
                        "username": "testuser",
                        "password": { "password": "testpass" }
                    }
                }
                """;

        // When
        Config config = readConfig(json);

        // Then
        assertThat(config.tls()).isNull();
    }

    @Test
    void tlsTrustInsecure() throws Exception {
        // Given
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

        // When
        Config config = readConfig(json);

        // Then
        assertThat(config)
                .isNotNull()
                .extracting(Config::tls)
                .isNotNull()
                .satisfies(tls -> assertThat(tls)
                        .extracting(Tls::trust)
                        .asInstanceOf(type(InsecureTls.class))
                        .extracting(InsecureTls::insecure)
                        .isEqualTo(true));
    }

    @Test
    void endpointUrlAndClientCredentials() throws IOException {
        // Given
        String json = """
                {
                    "endpointUrl": "https://ctm.example.com",
                    "clientCredentials": {
                        "clientId": "test-client-123"
                    },
                    "tls": {
                        "key": {
                            "storeFile": "/path/to/keystore.p12",
                            "storePassword": { "password": "storepass" }
                        }
                    }
                }
                """;

        // When
        Config config = readConfig(json);

        // Then
        assertThat(config.endpointUrl()).isEqualTo(URI.create("https://ctm.example.com"));
        assertThat(config.clientCredentials()).isNotNull();
        assertThat(config.clientCredentials().clientId()).isEqualTo("test-client-123");
        assertThat(config.tls()).isNotNull();
        assertThat(config.tls().key()).isNotNull();
    }

    @Test
    void clientCredentialsRequireClientCertificate() {
        // Given
        String json = """
                {
                    "endpointUrl": "https://ctm.example.com",
                    "clientCredentials": {
                        "clientId": "test-client-123"
                    }
                }
                """;

        // When/Then
        assertThatThrownBy(() -> readConfig(json))
                .isInstanceOf(ValueInstantiationException.class)
                .hasMessageContaining("clientCredentials requires a client certificate");
    }

    @Test
    void clientCredentialsRequireClientId() {
        // Given
        String json = """
                {
                    "endpointUrl": "https://ctm.example.com",
                    "clientCredentials": {},
                    "tls": {
                        "key": {
                            "storeFile": "/path/to/keystore.p12",
                            "storePassword": { "password": "storepass" }
                        }
                    }
                }
                """;

        // When/Then
        assertThatThrownBy(() -> readConfig(json))
                .isInstanceOf(MismatchedInputException.class)
                .hasMessageContaining("clientId");
    }

    @Test
    void cannotConfigureBothUserAndClientCredentials() {
        // Given
        String json = """
                {
                    "endpointUrl": "https://ctm.example.com",
                    "userCredentials": {
                        "username": "testuser",
                        "password": { "password": "testpass" }
                    },
                    "clientCredentials": {
                        "clientId": "test-client-123"
                    },
                    "tls": {
                        "key": {
                            "storeFile": "/path/to/keystore.p12",
                            "storePassword": { "password": "storepass" }
                        }
                    }
                }
                """;

        // When/Then
        assertThatThrownBy(() -> readConfig(json))
                .isInstanceOf(ValueInstantiationException.class)
                .hasMessageContaining("Cannot configure both userCredentials and clientCredentials");
    }

    private Config readConfig(String json) throws IOException {
        return MAPPER.reader().readValue(json, Config.class);
    }

}
