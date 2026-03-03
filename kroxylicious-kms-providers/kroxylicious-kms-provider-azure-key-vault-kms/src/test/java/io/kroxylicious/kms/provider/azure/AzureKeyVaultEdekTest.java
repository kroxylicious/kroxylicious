/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.azure;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.kroxylicious.kms.provider.azure.keyvault.SupportedKeyType;

import static java.util.stream.Collectors.joining;
import static java.util.stream.IntStream.range;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.argumentSet;

class AzureKeyVaultEdekTest {

    private static final String KEY_VERSION = "78deebed173b48e48f55abf87ed4cf71";
    private static final byte[] EDEK = { 1, 2, 3 };
    private static final String KEY_NAME = "key-name";
    private static final String VAULT_NAME = "myvault";
    private static final SupportedKeyType KEY_TYPE = SupportedKeyType.RSA;

    @Test
    void testKeyVersion128Bits() {
        AzureKeyVaultEdek azureKeyVaultEdek = createEdek(KEY_NAME, KEY_VERSION, EDEK, VAULT_NAME, KEY_TYPE);
        Optional<byte[]> bytes = azureKeyVaultEdek.keyVersion128bit();
        assertThat(bytes).isPresent().hasValueSatisfying(bytes1 -> assertThat(bytes1).hasSize(16)
                .containsExactly(120, -34, -21, -19, 23, 59, 72, -28, -113, 85, -85, -8, 126, -44, -49, 113));
    }

    @Test
    void testKeyVersion128BitsWhenNonHex() {
        AzureKeyVaultEdek azureKeyVaultEdek = createEdek(KEY_NAME, repeatString(32, "z"), EDEK, VAULT_NAME, KEY_TYPE);
        Optional<byte[]> bytes = azureKeyVaultEdek.keyVersion128bit();
        assertThat(bytes).isEmpty();
    }

    static Stream<Arguments> validEdek() {
        return Stream.of(argumentSet("simple", KEY_NAME, KEY_VERSION, EDEK),
                argumentSet("key name with uppercase alpha", "ABCDEFGHIJKLMNOPQRSTUVWXYZ", KEY_VERSION, EDEK),
                argumentSet("key name with lowercase alpha", "abcdefghijklmnopqrstuvwxyz", KEY_VERSION, EDEK),
                argumentSet("key name with 0-9", "0123456789", KEY_VERSION, EDEK),
                argumentSet("key name with hyphen", "-", KEY_VERSION, EDEK));
    }

    @MethodSource
    @ParameterizedTest
    void validEdek(String keyName, String keyVersion, byte[] edek) {
        AzureKeyVaultEdek azureKeyVaultEdek = createEdek(keyName, keyVersion, edek, VAULT_NAME, KEY_TYPE);
        assertThat(azureKeyVaultEdek.keyName()).isEqualTo(keyName);
        assertThat(azureKeyVaultEdek.keyVersion()).isEqualTo(keyVersion);
        assertThat(azureKeyVaultEdek.edek()).containsExactly(edek);
    }

    static Stream<Arguments> invalidKeyName() {
        return Stream.of(argumentSet("null keyName", null, NullPointerException.class, "keyName must not be null"),
                argumentSet("empty keyName", "", IllegalArgumentException.class, "keyName cannot be blank"),
                argumentSet("blank keyName", " ", IllegalArgumentException.class, "keyName cannot be blank"),
                argumentSet("keyname contains exotic character", "_", IllegalArgumentException.class, "keyName is not valid by pattern ^[a-zA-Z0-9-]+$"),
                argumentSet("keyname greater than 127 characters", repeatString(128, "a"), IllegalArgumentException.class, "keyName length cannot be longer than 127"));
    }

    @MethodSource
    @ParameterizedTest
    void invalidKeyName(String keyName, Class<? extends Exception> expectedException, String expectedMessage) {
        assertThatThrownBy(() -> createEdek(keyName, KEY_VERSION, EDEK, VAULT_NAME, KEY_TYPE)).isInstanceOf(expectedException)
                .hasMessageContaining(expectedMessage);
    }

    static Stream<Arguments> invalidKeyVersion() {
        return Stream.of(argumentSet("null keyVersion", null, NullPointerException.class, "keyVersion must not be null"),
                argumentSet("empty keyVersion", "", IllegalArgumentException.class, "keyVersion cannot be blank"),
                argumentSet("blank keyVersion", " ", IllegalArgumentException.class, "keyVersion cannot be blank"),
                argumentSet("keyVersion greater than 32 characters", repeatString(33, "a"), IllegalArgumentException.class, "keyVersion must be 32 characters long"),
                argumentSet("keyVersion less than 32 characters", repeatString(31, "a"), IllegalArgumentException.class, "keyVersion must be 32 characters long"));
    }

    @MethodSource
    @ParameterizedTest
    void invalidKeyVersion(String keyVersion, Class<? extends Exception> expectedException, String expectedMessage) {
        assertThatThrownBy(() -> createEdek(KEY_NAME, keyVersion, EDEK, VAULT_NAME, KEY_TYPE)).isInstanceOf(expectedException)
                .hasMessageContaining(expectedMessage);
    }

    static Stream<Arguments> invalidEdek() {
        return Stream.of(argumentSet("null edek", null, NullPointerException.class, "edek must not be null"),
                argumentSet("empty edek", new byte[]{}, IllegalArgumentException.class, "edek cannot be empty"));
    }

    @MethodSource
    @ParameterizedTest
    void invalidEdek(byte[] edek, Class<? extends Exception> expectedException, String expectedMessage) {
        assertThatThrownBy(() -> createEdek(KEY_NAME, KEY_VERSION, edek, VAULT_NAME, KEY_TYPE)).isInstanceOf(expectedException)
                .hasMessageContaining(expectedMessage);
    }

    /**
     * EDEK implementations are used as cache keys in the Record Encryption Filter, we want to be certain that equals and hash work as we expect
     */
    @MethodSource
    @ParameterizedTest
    void testEqualsHashCode(AzureKeyVaultEdek a, AzureKeyVaultEdek b, boolean shouldEqual) {
        if (shouldEqual) {
            assertThat(a).isEqualTo(b);
            assertThat(b).isEqualTo(a);
            assertThat(a).hasSameHashCodeAs(b);
        }
        else {
            assertThat(a).isNotEqualTo(b);
            assertThat(b).isNotEqualTo(a);
        }
    }

    static List<Arguments> testEqualsHashCode() {
        AzureKeyVaultEdek baseline = createEdek(KEY_NAME, KEY_VERSION, EDEK, VAULT_NAME, KEY_TYPE);
        AzureKeyVaultEdek baselineClone = createEdek(KEY_NAME, KEY_VERSION, EDEK.clone(), VAULT_NAME, KEY_TYPE);
        AzureKeyVaultEdek differentName = createEdek("another-name", KEY_VERSION, EDEK, VAULT_NAME, KEY_TYPE);
        AzureKeyVaultEdek differentVersion = createEdek(KEY_NAME, "a".repeat(32), EDEK, VAULT_NAME, KEY_TYPE);
        AzureKeyVaultEdek differentEdek = createEdek(KEY_NAME, KEY_VERSION, new byte[]{ 4, 5, 6 }, VAULT_NAME, KEY_TYPE);
        AzureKeyVaultEdek differentVaultName = createEdek(KEY_NAME, KEY_VERSION, EDEK, "anotherVault", KEY_TYPE);
        AzureKeyVaultEdek differentKeyType = createEdek(KEY_NAME, KEY_VERSION, EDEK, VAULT_NAME, SupportedKeyType.OCT);
        List<Arguments> arguments = new ArrayList<>();
        arguments.add(argumentSet("baseline equals itself", baseline, baseline, true));
        arguments.add(argumentSet("edek deep-equals", baseline, baselineClone, true)); // ensure they don't have to be the same array instance
        arguments.add(argumentSet("not equal when key names differ", baseline, differentName, false));
        arguments.add(argumentSet("not equal when key versions differ", baseline, differentVersion, false));
        arguments.add(argumentSet("not equal when edeks differ", baseline, differentEdek, false));
        arguments.add(argumentSet("not equal when vault names differ", baseline, differentVaultName, false));
        arguments.add(argumentSet("not equal when key types differ", baseline, differentKeyType, false));
        return arguments;
    }

    private static AzureKeyVaultEdek createEdek(String keyName, String keyVersion, byte[] edek, String vaultName, SupportedKeyType keyType) {
        return new AzureKeyVaultEdek(keyName, keyVersion, edek, vaultName, keyType);
    }

    static String repeatString(int times, String string) {
        return range(0, times).mapToObj(i -> string).collect(joining(""));
    }

}