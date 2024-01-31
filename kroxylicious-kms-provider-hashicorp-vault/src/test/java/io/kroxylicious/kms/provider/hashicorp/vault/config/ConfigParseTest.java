/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.hashicorp.vault.config;

import java.io.IOException;
import java.net.URI;

import javax.net.ssl.SSLContext;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import com.fasterxml.jackson.databind.exc.ValueInstantiationException;

import io.kroxylicious.proxy.config.tls.InsecureTls;
import io.kroxylicious.proxy.config.tls.Tls;

import static org.assertj.core.api.Assertions.assertThat;

class ConfigParseTest {
    ObjectMapper mapper = new ObjectMapper();

    @Test
    void testVaultUrlAndToken() throws IOException {
        String json = """
                {
                    "vaultUrl": "http://vault",
                    "vaultToken": "token"
                }
                """;
        Config config = readConfig(json);
        assertThat(config.vaultToken()).isEqualTo("token");
        assertThat(config.vaultUrl()).isEqualTo(URI.create("http://vault"));
    }

    @Test
    void testVaultUrlRequired() {
        Assertions.assertThatThrownBy(() -> {
            String json = """
                    {
                        "vaultToken": "token"
                    }
                    """;
            readConfig(json);
        }).isInstanceOf(MismatchedInputException.class).hasMessageContaining("vaultUrl");
    }

    @Test
    void testVaultUrlShouldNotBeNull() {
        Assertions.assertThatThrownBy(() -> {
            String json = """
                    {
                        "vaultUrl": null,
                        "vaultToken": "token"
                    }
                    """;
            readConfig(json);
        }).isInstanceOf(ValueInstantiationException.class).cause().isInstanceOf(NullPointerException.class);
    }

    @Test
    void testVaultTokenRequired() {
        Assertions.assertThatThrownBy(() -> {
            String json = """
                    {
                        "vaultUrl": "https://vault"
                    }
                    """;
            readConfig(json);
        }).isInstanceOf(MismatchedInputException.class).hasMessageContaining("vaultToken");
    }

    @Test
    void testVaultTokenShouldNotBeNull() {
        Assertions.assertThatThrownBy(() -> {
            String json = """
                    {
                        "vaultUrl": "https://vault",
                        "vaultToken": null
                    }
                    """;
            readConfig(json);
        }).isInstanceOf(ValueInstantiationException.class).cause().isInstanceOf(NullPointerException.class);
    }

    @Test
    void testEmptyTls() throws Exception {
        String json = """
                {
                    "vaultUrl": "https://vault",
                    "vaultToken": "token",
                    "tls": {}
                }
                """;
        Config config = readConfig(json);
        assertThat(config.tls()).isNotNull();
        assertThat(config.tls().trust()).isNull();
        assertThat(config.sslContext()).isSameAs(SSLContext.getDefault());
    }

    @Test
    void testMissingTls() throws Exception {
        String json = """
                {
                    "vaultUrl": "https://vault",
                    "vaultToken": "token"
                }
                """;
        Config config = readConfig(json);
        assertThat(config.tls()).isNull();
        assertThat(config.sslContext()).isSameAs(SSLContext.getDefault());
    }

    // we do not need to exhaustively test serialization of Tls as it has its own coverage
    @Test
    void testTlsTrust() throws Exception {
        String json = """
                {
                    "vaultUrl": "https://vault",
                    "vaultToken": "token",
                    "tls": {
                        "trust": {
                            "insecure": true
                        }
                    }
                }
                """;
        Config config = readConfig(json);
        Config expected = new Config(URI.create("https://vault"), "token", new Tls(null, new InsecureTls(true)));
        assertThat(config).isEqualTo(expected);
    }

    private Config readConfig(String json) throws IOException {
        return mapper.reader().readValue(json, Config.class);
    }

}
