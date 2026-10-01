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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import com.fasterxml.jackson.databind.exc.ValueInstantiationException;

import io.kroxylicious.proxy.config.secret.InlinePassword;
import io.kroxylicious.proxy.config.tls.InsecureTls;
import io.kroxylicious.proxy.config.tls.Tls;

import static org.assertj.core.api.Assertions.assertThat;

class ConfigParseTest {
    private static final Logger LOG = LoggerFactory.getLogger(ConfigParseTest.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    // ---------------------------------------------------------------------------
    // Deprecated top-level vaultToken (backward-compatibility)
    // ---------------------------------------------------------------------------

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
        assertThat(config.credentials()).isNotNull();
        assertThat(config.credentials().token()).isNotNull();
        assertThat(config.credentials().token().token().getProvidedPassword()).isEqualTo("token");
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
            assertThat(config.credentials()).isNotNull();
            assertThat(config.credentials().token().token().getProvidedPassword()).isEqualTo("token");
        }
        finally {
            if (!tmp.toFile().delete()) {
                LOG.warn("Could not delete {}", tmp.toFile().getAbsolutePath());
            }
        }
    }

    // ---------------------------------------------------------------------------
    // New credentials.token structure
    // ---------------------------------------------------------------------------

    @Test
    void credentialsTokenInlinePassword() throws IOException {
        String json = """
                {
                    "vaultTransitEngineUrl": "http://vault",
                    "credentials": {
                        "token": {
                            "token": { "password": "mytoken" }
                        }
                    }
                }
                """;
        Config config = readConfig(json);
        assertThat(config.credentials()).isNotNull();
        assertThat(config.credentials().token()).isNotNull();
        assertThat(config.credentials().token().token().getProvidedPassword()).isEqualTo("mytoken");
        assertThat(config.credentials().kubernetes()).isNull();
        assertThat(config.vaultToken()).isNull();
    }

    // ---------------------------------------------------------------------------
    // New credentials.kubernetes structure
    // ---------------------------------------------------------------------------

    @Test
    void credentialsKubernetesWithRoleOnly() throws IOException {
        String json = """
                {
                    "vaultTransitEngineUrl": "http://vault",
                    "credentials": {
                        "kubernetes": {
                            "role": "my-k8s-role"
                        }
                    }
                }
                """;
        Config config = readConfig(json);
        assertThat(config.credentials()).isNotNull();
        assertThat(config.credentials().kubernetes()).isNotNull();
        assertThat(config.credentials().kubernetes().role()).isEqualTo("my-k8s-role");
        assertThat(config.credentials().kubernetes().serviceAccountTokenPath())
                .isEqualTo(KubernetesCredentialsConfig.DEFAULT_SERVICE_ACCOUNT_TOKEN_PATH);
        assertThat(config.credentials().kubernetes().authPath())
                .isEqualTo(KubernetesCredentialsConfig.DEFAULT_AUTH_PATH);
        assertThat(config.credentials().token()).isNull();
        assertThat(config.vaultToken()).isNull();
    }

    @Test
    void credentialsKubernetesWithCustomPaths() throws IOException {
        String json = """
                {
                    "vaultTransitEngineUrl": "http://vault",
                    "credentials": {
                        "kubernetes": {
                            "role": "my-k8s-role",
                            "serviceAccountTokenPath": "/custom/sa/token",
                            "authPath": "k8s"
                        }
                    }
                }
                """;
        Config config = readConfig(json);
        var k8s = config.credentials().kubernetes();
        assertThat(k8s.role()).isEqualTo("my-k8s-role");
        assertThat(k8s.serviceAccountTokenPath()).isEqualTo("/custom/sa/token");
        assertThat(k8s.authPath()).isEqualTo("k8s");
    }

    // ---------------------------------------------------------------------------
    // Validation errors
    // ---------------------------------------------------------------------------

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
    void credentialsOrDeprecatedTokenRequired() {
        Assertions.assertThatThrownBy(() -> {
            String json = """
                    {
                        "vaultTransitEngineUrl": "https://vault"
                    }
                    """;
            readConfig(json);
        }).isInstanceOf(ValueInstantiationException.class).cause().isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Either 'credentials' or deprecated 'vaultToken' must be provided");
    }

    @Test
    void vaultTokenShouldNotBeNullWhenCredentialsMissing() {
        Assertions.assertThatThrownBy(() -> {
            String json = """
                    {
                        "vaultTransitEngineUrl": "https://vault",
                        "vaultToken": null
                    }
                    """;
            readConfig(json);
        }).isInstanceOf(ValueInstantiationException.class).cause().isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Either 'credentials' or deprecated 'vaultToken' must be provided");
    }

    @Test
    void bothCredentialsAndDeprecatedTokenThrows() {
        Assertions.assertThatThrownBy(() -> {
            String json = """
                    {
                        "vaultTransitEngineUrl": "https://vault",
                        "vaultToken": { "password" : "token" },
                        "credentials": {
                            "token": { "token": { "password": "token2" } }
                        }
                    }
                    """;
            readConfig(json);
        }).isInstanceOf(ValueInstantiationException.class).cause().isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Cannot specify both 'vaultToken' and 'credentials' - use 'credentials.token' instead");
    }

    @Test
    void credentialsWithBothTokenAndKubernetesThrows() {
        Assertions.assertThatThrownBy(() -> {
            String json = """
                    {
                        "vaultTransitEngineUrl": "https://vault",
                        "credentials": {
                            "token": { "token": { "password": "mytoken" } },
                            "kubernetes": { "role": "my-role" }
                        }
                    }
                    """;
            readConfig(json);
        }).isInstanceOf(ValueInstantiationException.class).cause().isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Only one of 'token' or 'kubernetes' credentials may be provided");
    }

    @Test
    void credentialsWithNeitherTokenNorKubernetesThrows() {
        Assertions.assertThatThrownBy(() -> {
            String json = """
                    {
                        "vaultTransitEngineUrl": "https://vault",
                        "credentials": {}
                    }
                    """;
            readConfig(json);
        }).isInstanceOf(ValueInstantiationException.class).cause().isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Either 'token' or 'kubernetes' credentials must be provided");
    }

    // ---------------------------------------------------------------------------
    // TLS
    // ---------------------------------------------------------------------------

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
    }

    @Test
    void insecureTls() throws IOException {
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
        Config expected = new Config(URI.create("https://vault"), new InlinePassword("token"), new Tls(null, new InsecureTls(true), null, null));
        assertThat(config).isEqualTo(expected);
    }

    private Config readConfig(String json) throws IOException {
        return MAPPER.reader().readValue(json, Config.class);
    }

}
