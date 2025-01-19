/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.hashicorp.vault.config;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import com.fasterxml.jackson.databind.exc.ValueInstantiationException;

import io.kroxylicious.proxy.config.secret.InlinePassword;
import io.kroxylicious.proxy.config.tls.InsecureTls;
import io.kroxylicious.proxy.config.tls.Tls;

import static org.assertj.core.api.Assertions.assertThat;

class ConfigParseTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void vaultUrlAndInlineToken() throws IOException {
        String json = """
                {
                    "vaultTransitEngineUrl": "http://vault",
                    "vaultToken": { "password" : "token" }
                }
                """;
        Config config = readConfig(json);
        assertThat(config.vaultToken().getProvidedPassword()).isEqualTo("token");
        assertThat(config.vaultTransitEngineUrl()).isEqualTo(URI.create("http://vault"));
    }

    @Test
    void tokenFromPasswordFile() throws IOException {
        var tmp = Files.createTempFile("password", "txt");
        tmp.toFile().deleteOnExit();
        Files.writeString(tmp, "token");

        try {
            String json = """
                    {
                        "vaultTransitEngineUrl": "http://vault",
                        "vaultToken": { "passwordFile" : "%s" }
                    }
                    """.formatted(tmp);
            Config config = readConfig(json);
            assertThat(config.vaultToken().getProvidedPassword()).isEqualTo("token");
            assertThat(config.vaultTransitEngineUrl()).isEqualTo(URI.create("http://vault"));
        }
        finally {
            var unused = tmp.toFile().delete();
        }
    }

    @Test
    void vaultUrlRequired() {
        Assertions.assertThatThrownBy(() -> {
            String json = """
                    {
                        "vaultToken": { "password" : "token" }
                    }
                    """;
            readConfig(json);
        }).isInstanceOf(MismatchedInputException.class).hasMessageContaining("vaultTransitEngineUrl");
    }

    @Test
    void vaultUrlShouldNotBeNull() {
        Assertions.assertThatThrownBy(() -> {
            String json = """
                    {
                        "vaultTransitEngineUrl": null,
                        "vaultToken": { "password" : "token" }
                    }
                    """;
            readConfig(json);
        }).isInstanceOf(ValueInstantiationException.class).cause().isInstanceOf(NullPointerException.class);
    }

    @Test
    void vaultTokenRequired() {
        Assertions.assertThatThrownBy(() -> {
            String json = """
                    {
                        "vaultTransitEngineUrl": "https://vault"
                    }
                    """;
            readConfig(json);
        }).isInstanceOf(MismatchedInputException.class).hasMessageContaining("vaultToken");
    }

    @Test
    void vaultTokenShouldNotBeNull() {
        Assertions.assertThatThrownBy(() -> {
            String json = """
                    {
                        "vaultTransitEngineUrl": "https://vault",
                        "vaultToken": null
                    }
                    """;
            readConfig(json);
        }).isInstanceOf(ValueInstantiationException.class).cause().isInstanceOf(NullPointerException.class);
    }

    @Test
    void emptyTls() throws Exception {
        String json = """
                {
                    "vaultTransitEngineUrl": "https://vault",
                    "vaultToken": { "password" : "token" },
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
                    "vaultTransitEngineUrl": "https://vault",
                    "vaultToken": { "password" : "token" }
                }
                """;
        Config config = readConfig(json);
        assertThat(config.tls()).isNull();
    }

    // we do not need to exhaustively test serialization of Tls as it has its own coverage
    @Test
    void testTlsTrust() throws Exception {
        String json = """
                {
                    "vaultTransitEngineUrl": "https://vault",
                    "vaultToken": { "password" : "token" },
                    "tls": {
                        "trust": {
                            "insecure": true
                        }
                    }
                }
                """;
        Config config = readConfig(json);
        Config expected = new Config(URI.create("https://vault"), new InlinePassword("token"), new Tls(null, new InsecureTls(true)));
        assertThat(config).isEqualTo(expected);
    }

    private Config readConfig(String json) throws IOException {
        return MAPPER.reader().readValue(json, Config.class);
    }

}
