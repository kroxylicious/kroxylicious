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
        assertThat(config.credentials().vaultToken()).isNotNull();
        assertThat(config.credentials().vaultToken().token().getProvidedPassword()).isEqualTo("token");
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
            assertThat(config.credentials().vaultToken().token().getProvidedPassword()).isEqualTo("token");
        }
        finally {
            if (!tmp.toFile().delete()) {
                LOG.warn("Could not delete {}", tmp.toFile().getAbsolutePath());
            }
        }
    }

    @Test
    void deprecatedVaultTransitEngineUrlExtraction() throws IOException {
        String json = """
                {
                    "vaultTransitEngineUrl": "http://vault:8200/v1/my-namespace/custom-transit",
                    "vaultToken": { "password" : "token" }
                }
                """;
        Config config = readConfig(json);
        assertThat(config.vaultUrl()).isEqualTo(URI.create("http://vault:8200"));
        assertThat(config.transitEnginePath()).isEqualTo("my-namespace/custom-transit");
        assertThat(config.credentials().vaultToken().token().getProvidedPassword()).isEqualTo("token");
    }

    // ---------------------------------------------------------------------------
    // New credentials.vaultToken structure
    // ---------------------------------------------------------------------------

    @Test
    void credentialsVaultTokenInlinePassword() throws IOException {
        String json = """
                {
                    "vaultUrl": "http://vault:8200",
                    "credentials": {
                        "vaultToken": {
                            "token": { "password": "mytoken" }
                        }
                    }
                }
                """;
        Config config = readConfig(json);
        assertThat(config.credentials()).isNotNull();
        assertThat(config.credentials().vaultToken()).isNotNull();
        assertThat(config.credentials().vaultToken().token().getProvidedPassword()).isEqualTo("mytoken");
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
                    "vaultUrl": "http://vault:8200",
                    "credentials": {
                        "kubernetes": {
                            "vaultRole": "my-k8s-role"
                        }
                    }
                }
                """;
        Config config = readConfig(json);
        assertThat(config.credentials()).isNotNull();
        assertThat(config.credentials().kubernetes()).isNotNull();
        assertThat(config.credentials().kubernetes().vaultRole()).isEqualTo("my-k8s-role");
        assertThat(config.credentials().kubernetes().serviceAccountTokenFile())
                .isEqualTo(KubernetesCredentialsConfig.DEFAULT_SERVICE_ACCOUNT_TOKEN_FILE);
        assertThat(config.credentials().kubernetes().authPath())
                .isEqualTo(KubernetesCredentialsConfig.DEFAULT_AUTH_PATH);
        assertThat(config.credentials().vaultToken()).isNull();
        assertThat(config.vaultToken()).isNull();
    }

    @Test
    void credentialsKubernetesWithCustomPaths() throws IOException {
        String json = """
                {
                    "vaultUrl": "http://vault:8200",
                    "credentials": {
                        "kubernetes": {
                            "vaultRole": "my-k8s-role",
                            "serviceAccountTokenFile": "/custom/sa/token",
                            "authPath": "k8s"
                        }
                    }
                }
                """;
        Config config = readConfig(json);
        var k8s = config.credentials().kubernetes();
        assertThat(k8s.vaultRole()).isEqualTo("my-k8s-role");
        assertThat(k8s.serviceAccountTokenFile()).isEqualTo("/custom/sa/token");
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
        }).isInstanceOf(ValueInstantiationException.class).cause().isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Either 'vaultUrl' or deprecated 'vaultTransitEngineUrl' must be provided");
    }

    @Test
    void vaultUrlShouldNotBeNull() {
        Assertions.assertThatThrownBy(() -> {
            String json = """
                    {
                        "vaultUrl": null,
                        "vaultToken": { "password" : "token" }
                    }
                    """;
            readConfig(json);
        }).isInstanceOf(ValueInstantiationException.class).cause().isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Either 'vaultUrl' or deprecated 'vaultTransitEngineUrl' must be provided");
    }

    @Test
    void bothVaultUrlAndVaultTransitEngineUrlThrows() {
        Assertions.assertThatThrownBy(() -> {
            String json = """
                    {
                        "vaultUrl": "https://vault",
                        "vaultTransitEngineUrl": "https://vault/v1/transit",
                        "credentials": {
                            "vaultToken": { "token": { "password": "mytoken" } }
                        }
                    }
                    """;
            readConfig(json);
        }).isInstanceOf(ValueInstantiationException.class).cause().isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Cannot mix deprecated 'vaultTransitEngineUrl' with modern 'credentials'");
    }

    @Test
    void modernVaultUrlVaultNamespaceAndTransitEnginePath() throws IOException {
        String json = """
                {
                    "vaultUrl": "https://myvault:8200",
                    "vaultNamespace": "my-namespace",
                    "transitEnginePath": "custom-transit",
                    "credentials": {
                        "vaultToken": {
                            "token": { "password": "mytoken" }
                        }
                    }
                }
                """;
        Config config = readConfig(json);
        assertThat(config.vaultUrl()).isEqualTo(URI.create("https://myvault:8200"));
        assertThat(config.vaultNamespace()).isEqualTo("my-namespace");
        assertThat(config.transitEnginePath()).isEqualTo("custom-transit");
    }

    @Test
    void legacyUrlAndModernCredentialsThrows() {
        Assertions.assertThatThrownBy(() -> {
            String json = """
                    {
                        "vaultTransitEngineUrl": "https://vault/v1/transit",
                        "credentials": {
                            "vaultToken": { "token": { "password": "token" } }
                        }
                    }
                    """;
            readConfig(json);
        }).isInstanceOf(ValueInstantiationException.class).cause().isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Cannot mix deprecated 'vaultTransitEngineUrl' with modern 'credentials'");
    }

    @Test
    void modernUrlAndLegacyTokenThrows() {
        Assertions.assertThatThrownBy(() -> {
            String json = """
                    {
                        "vaultUrl": "https://vault:8200",
                        "vaultToken": { "password" : "token" }
                    }
                    """;
            readConfig(json);
        }).isInstanceOf(ValueInstantiationException.class).cause().isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Cannot mix modern 'vaultUrl' with deprecated 'vaultToken'");
    }

    @Test
    void bothCredentialsAndDeprecatedTokenThrows() {
        Assertions.assertThatThrownBy(() -> {
            String json = """
                    {
                        "vaultUrl": "https://vault:8200",
                        "vaultToken": { "password" : "token" },
                        "credentials": {
                            "vaultToken": { "token": { "password": "token2" } }
                        }
                    }
                    """;
            readConfig(json);
        }).isInstanceOf(ValueInstantiationException.class).cause().isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Cannot mix modern 'vaultUrl' with deprecated 'vaultToken'");
    }

    @Test
    void credentialsWithBothVaultTokenAndKubernetesThrows() {
        Assertions.assertThatThrownBy(() -> {
            String json = """
                    {
                        "vaultUrl": "https://vault:8200",
                        "credentials": {
                            "vaultToken": { "token": { "password": "mytoken" } },
                            "kubernetes": { "vaultRole": "my-role" }
                        }
                    }
                    """;
            readConfig(json);
        }).isInstanceOf(ValueInstantiationException.class).cause().isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Exactly one of 'vaultToken' or 'kubernetes' credentials must be provided");
    }

    @Test
    void credentialsWithNeitherVaultTokenNorKubernetesThrows() {
        Assertions.assertThatThrownBy(() -> {
            String json = """
                    {
                        "vaultUrl": "https://vault:8200",
                        "credentials": {}
                    }
                    """;
            readConfig(json);
        }).isInstanceOf(ValueInstantiationException.class).cause().isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Exactly one of 'vaultToken' or 'kubernetes' credentials must be provided");
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
