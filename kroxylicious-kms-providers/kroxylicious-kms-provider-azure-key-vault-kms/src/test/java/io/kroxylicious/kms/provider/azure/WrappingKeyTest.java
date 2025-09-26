/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.azure;

import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

import io.kroxylicious.kms.provider.azure.keyvault.SupportedKeyType;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

import static io.kroxylicious.kms.provider.azure.keyvault.SupportedKeyType.OCT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WrappingKeyTest {

    static final String KEY_NAME = "name";
    static final String KEY_VERSION = "version";

    @Test
    void keyVersion() {
        WrappingKey wrappingKey = new WrappingKey(KEY_NAME, KEY_VERSION, OCT, "myvault");
        assertThat(wrappingKey.keyName()).isEqualTo(KEY_NAME);
        assertThat(wrappingKey.keyVersion()).isEqualTo(KEY_VERSION);
        assertThat(wrappingKey.supportedKeyType()).isEqualTo(OCT);
    }

    @CsvSource(nullValues = "null", value = { "null, version, OCT, keyName is null",
            "name, null, OCT, keyVersion is null, name, version, null, supportedKeyType is null" })
    @ParameterizedTest
    void invalidConstructorArgs(@Nullable String name, @Nullable String version, @Nullable String supportedKeyType, String message) {
        SupportedKeyType supportedKeyType1 = supportedKeyType == null ? null : SupportedKeyType.valueOf(supportedKeyType);
        assertThatThrownBy(() -> new WrappingKey(name, version, supportedKeyType1, "myvault")).isInstanceOf(
                NullPointerException.class).hasMessage(message);
    }

    @Test
    void parse() {
        WrappingKey name = WrappingKey.parse("CreateSoftKeyTest", "https://myvault.vault.azure.net/keys/CreateSoftKeyTest/78deebed173b48e48f55abf87ed4cf71", OCT);
        assertThat(name.keyName()).isEqualTo("CreateSoftKeyTest");
        assertThat(name.keyVersion()).isEqualTo("78deebed173b48e48f55abf87ed4cf71");
        assertThat(name.vaultName()).isEqualTo("myvault");
    }

    public static Stream<Arguments> parseVaultName() {
        String minLengthVaultName = "a".repeat(3);
        String maxLengthVaultName = "a".repeat(24);
        return Stream.of(Arguments.argumentSet("min length", keyIdForVaultName(minLengthVaultName), minLengthVaultName),
                Arguments.argumentSet("max length", keyIdForVaultName(maxLengthVaultName), maxLengthVaultName),
                // divided in half because max length is 24
                Arguments.argumentSet("lowercase 1", keyIdForVaultName("abcdefghijklm"), "abcdefghijklm"),
                Arguments.argumentSet("lowercase 2", keyIdForVaultName("nopqrstuvwxyz"), "nopqrstuvwxyz"),
                Arguments.argumentSet("capitals 1", keyIdForVaultName("ABCDEFGHIJKLM"), "ABCDEFGHIJKLM"),
                Arguments.argumentSet("capitals 2", keyIdForVaultName("NOPQRSTUVWXYZ"), "NOPQRSTUVWXYZ"),
                Arguments.argumentSet("hyphen", keyIdForVaultName("a-b"), "a-b"),
                Arguments.argumentSet("numeric", keyIdForVaultName("0123456789"), "0123456789"));
    }

    @NonNull
    private static String keyIdForVaultName(String minLengthVaultName) {
        return "https://" + minLengthVaultName + ".vault.com/keys/key/78";
    }

    @MethodSource
    @ParameterizedTest
    void parseVaultName(String keyId, String expectedVaultName) {
        WrappingKey name = WrappingKey.parse("arbitrary", keyId, OCT);
        assertThat(name.vaultName()).isEqualTo(expectedVaultName);
    }

    public static Stream<Arguments> parseInvalidVaultName() {
        String tooShortVaultName = "a".repeat(2);
        String tooLongVaultName = "a".repeat(25);
        return Stream.of(Arguments.argumentSet("too short", keyIdForVaultName(tooShortVaultName),
                "vault name cannot be obtained from host " + tooShortVaultName + ".vault.com it must match ^([a-zA-Z0-9\\-]{3,24})\\..+"),
                Arguments.argumentSet("too long", keyIdForVaultName(tooLongVaultName),
                        "vault name cannot be obtained from host " + tooLongVaultName + ".vault.com it must match ^([a-zA-Z0-9\\-]{3,24})\\..+"),
                Arguments.argumentSet("contains consecutive hyphens", keyIdForVaultName("a--b"), "vault name cannot contain consecutive hyphens a--b"),
                // sanity checks, we are already restricted to a subset of valid domain name labels. They cannot begin or end with hyphens, or
                // contain characters other than a-z (insensitive), 0-9 and hyphen.
                Arguments.argumentSet("invalid label - contains underscore", keyIdForVaultName("a_b"),
                        "host is null"),
                Arguments.argumentSet("invalid label - starts with hyphen", keyIdForVaultName("-abc"),
                        "host is null"),
                Arguments.argumentSet("invalid label - ends with hyphen", keyIdForVaultName("abc-"),
                        "host is null"));
    }

    @MethodSource
    @ParameterizedTest
    void parseInvalidVaultName(String keyId, String expectedErrorMessage) {
        assertThatThrownBy(() -> WrappingKey.parse("arbitrary", keyId, OCT)).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(expectedErrorMessage);
    }

    @Test
    void parseNonUriId() {
        assertThatThrownBy(() -> WrappingKey.parse("CreateSoftKeyTest", "z banana/zc", OCT))
                .isInstanceOf(IllegalArgumentException.class).hasMessage("failed to parse keyId 'z banana/zc' as a URI");
    }

    @Test
    void parseUriWithNoSlashInPath() {
        assertThatThrownBy(() -> WrappingKey.parse("CreateSoftKeyTest", "http://my-kv.vault.azure.net", OCT))
                .isInstanceOf(IllegalArgumentException.class).hasMessage("keyId path '' must contain a '/'");
    }

    @Test
    void parseUriWithOnlySlashInPath() {
        assertThatThrownBy(() -> WrappingKey.parse("CreateSoftKeyTest", "http://my-kv.vault.azure.net/", OCT))
                .isInstanceOf(IllegalArgumentException.class).hasMessage("keyId path '/' must not end with '/'");
    }

    @Test
    void parseUriWithPathEndingWithSlash() {
        assertThatThrownBy(() -> WrappingKey.parse("CreateSoftKeyTest", "http://my-kv.vault.azure.net/abc/", OCT))
                .isInstanceOf(IllegalArgumentException.class).hasMessage("keyId path '/abc/' must not end with '/'");
    }

}
