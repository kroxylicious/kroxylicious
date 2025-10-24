/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.azure.config;

import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

import io.kroxylicious.kms.provider.azure.config.auth.EntraIdentityConfig;
import io.kroxylicious.proxy.config.secret.InlinePassword;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AzureKeyVaultConfigTest {

    private static final EntraIdentityConfig ENTRA_IDENTITY = new EntraIdentityConfig(null, "tenant", new InlinePassword("pazz"), new InlinePassword("pazz"), null, null);

    static Stream<Arguments> keyVaultNameValid() {
        return Stream.of(Arguments.argumentSet("minimum length", "a".repeat(3)),
                Arguments.argumentSet("lowercase 1", "abcdefghijklm"),
                Arguments.argumentSet("lowercase 2", "nopqrstuvwxyz"),
                Arguments.argumentSet("uppercase 1", "ABCDEFGHIJKLM"),
                Arguments.argumentSet("uppercase 2", "NOPQRSTUVWXYZ"),
                Arguments.argumentSet("hyphen", "a-b"),
                Arguments.argumentSet("non-consecutive hyphens", "a-b-c"),
                Arguments.argumentSet("maximum length", "a".repeat(24)));
    }

    @MethodSource
    @ParameterizedTest
    void keyVaultNameValid(String name) {
        AzureKeyVaultConfig azureKeyVaultConfig = new AzureKeyVaultConfig(ENTRA_IDENTITY, null, name, "vault.azure.net", null, null, null);
        assertThat(azureKeyVaultConfig.keyVaultName()).isEqualTo(name);
    }

    static Stream<Arguments> keyVaultNameInvalid() {
        return Stream.of(Arguments.argumentSet("too short", "a".repeat(2), "keyVaultName does not match pattern ^[a-zA-Z0-9\\-]{3,24}$"),
                Arguments.argumentSet("starts with hyphen", "-aa", "keyVaultName must not start with '-'"),
                Arguments.argumentSet("ends with hyphen", "aa-", "keyVaultName must not end with '-'"),
                Arguments.argumentSet("unexpected characters", "_=+", "keyVaultName does not match pattern ^[a-zA-Z0-9\\-]{3,24}$"),
                Arguments.argumentSet("blank", " ", "keyVaultName does not match pattern ^[a-zA-Z0-9\\-]{3,24}$"),
                Arguments.argumentSet("empty", "", "keyVaultName does not match pattern ^[a-zA-Z0-9\\-]{3,24}$"),
                Arguments.argumentSet("contains consecutive hyphens", "a--a", "keyVaultName must not contain '--'"),
                Arguments.argumentSet("too long", "a".repeat(25), "keyVaultName does not match pattern ^[a-zA-Z0-9\\-]{3,24}$"));
    }

    @MethodSource
    @ParameterizedTest
    void keyVaultNameInvalid(String name, String error) {
        assertThatThrownBy(() -> new AzureKeyVaultConfig(ENTRA_IDENTITY, null, name, "vault.azure.net", null, null, null))
                .isInstanceOf(IllegalArgumentException.class).hasMessage(error);
    }

    static Stream<Arguments> hostInvalid() {
        return Stream.of(Arguments.argumentSet("invalid characters", "_", "keyVaultHost is not a valid host"),
                Arguments.argumentSet("segment with hyphen start", "my.-a.vault.com", "keyVaultHost is not a valid host"),
                Arguments.argumentSet("segment with hyphen end", "my.a-.vault.com", "keyVaultHost is not a valid host"));
    }

    @MethodSource
    @ParameterizedTest
    void hostInvalid(String host, String error) {
        assertThatThrownBy(() -> new AzureKeyVaultConfig(ENTRA_IDENTITY, null, "default", host, null, null, null))
                .isInstanceOf(IllegalArgumentException.class).hasMessage(error);
    }

    @CsvSource({ "localhost", "vault.azure.net", "vault.azure.cn", "vault.usgovcloudapi.net", "vault.microsoftazure.de" })
    @ParameterizedTest
    void hostValid(String host) {
        AzureKeyVaultConfig config = new AzureKeyVaultConfig(ENTRA_IDENTITY, null, "default", host, null, null, null);
        assertThat(config.keyVaultHost()).isEqualTo(host);
    }

    @Test
    void portValid() {
        AzureKeyVaultConfig config = new AzureKeyVaultConfig(ENTRA_IDENTITY, null, "default", "localhost", null, 8080, null);
        assertThat(config.keyVaultPort()).isEqualTo(8080);
    }

    @Test
    void defaultScheme() {
        AzureKeyVaultConfig config = minimalConfig();
        assertThat(config.kvScheme()).isEqualTo("https");
    }

    @Test
    void defaultPort() {
        AzureKeyVaultConfig config = minimalConfig();
        assertThat(config.keyVaultPort()).isNull();
    }

    @CsvSource({ "0", "-1", "65536" })
    @ParameterizedTest
    void portInvalid(int port) {
        assertThatThrownBy(() -> new AzureKeyVaultConfig(ENTRA_IDENTITY, null, "default", "localhost", null, port, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("keyVaultPort must be in the range (1, 65535) inclusive");
    }

    @Test
    void schemeShouldNotEndInColonSlashSlash() {
        assertThatThrownBy(() -> new AzureKeyVaultConfig(ENTRA_IDENTITY, null, "default", "localhost", "http://", null, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("keyVaultScheme must not end with ://");
    }

    private static AzureKeyVaultConfig minimalConfig() {
        return new AzureKeyVaultConfig(ENTRA_IDENTITY, null, "default", "localhost", null, null, null);
    }

}