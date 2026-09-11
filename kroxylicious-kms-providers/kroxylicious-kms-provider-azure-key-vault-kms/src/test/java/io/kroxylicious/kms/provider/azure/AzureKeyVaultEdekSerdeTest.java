/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.azure;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

import io.kroxylicious.kms.provider.azure.keyvault.SupportedKeyType;
import io.kroxylicious.kms.service.KmsException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

class AzureKeyVaultEdekSerdeTest {

    private static final String KEY_NAME = "key-name";
    private static final String NON_HEX_VERSION = "ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ";
    private static final String UPPER_HEX_VERSION = "78DEEBED173B48e48f55abf87ed4cf71";
    private static final String NON_HEX_VERSION_WITH_UNICODE = "αβγδεζηθικλμνξοπρστυφχψωαβγδεζηθ";
    private static final String HEX_VERSION = "78deebed173b48e48f55abf87ed4cf71";
    private static final byte[] EDEK_BYTES = { 5, 4, 3 };
    public static final String VAULT_NAME = "myvault";
    public static final SupportedKeyType KEY_TYPE = SupportedKeyType.RSA;
    private static final AzureKeyVaultEdek UPPERCASE_HEX_VERSION_EDEK = new AzureKeyVaultEdek(KEY_NAME, UPPER_HEX_VERSION, EDEK_BYTES, VAULT_NAME, KEY_TYPE);
    private static final AzureKeyVaultEdek UNICODE_VERSION_EDEK = new AzureKeyVaultEdek(KEY_NAME, NON_HEX_VERSION_WITH_UNICODE, EDEK_BYTES, VAULT_NAME,
            KEY_TYPE);
    private static final AzureKeyVaultEdek NON_HEX_VERSION_EDEK = new AzureKeyVaultEdek(KEY_NAME, NON_HEX_VERSION, EDEK_BYTES, VAULT_NAME, KEY_TYPE);
    private static final AzureKeyVaultEdek LOWER_CASE_HEX_VERSION_EDEK = new AzureKeyVaultEdek(KEY_NAME, HEX_VERSION, EDEK_BYTES, VAULT_NAME, KEY_TYPE);
    private static final String V0_LOWER_CASE_HEX_VERSION_BINARY = "AAhrZXktbmFtZQdteXZhdWx0AHje6+0XO0jkj1Wr+H7Uz3EFBAM=";
    private static final String V1_NON_HEX_VERSION_BINARY = "AQhrZXktbmFtZQdteXZhdWx0ACBaWlpaWlpaWlpaWlpaWlpaWlpaWlpaWlpaWlpaWlpaWgUEAw==";
    private static final String V1_UNICODE_VERSION_BINARY = "AQhrZXktbmFtZQdteXZhdWx0AEDOsc6yzrPOtM61zrbOt864zrnOus67zrzOvc6+zr/PgM+Bz4PPhM+Fz4bPh8+Iz4nOsc6yzrPOtM61zrbOt864BQQD";
    private static final String V1_UPPERCASE_HEX_VERSION_BINARY = "AQhrZXktbmFtZQdteXZhdWx0ACA3OERFRUJFRDE3M0I0OGU0OGY1NWFiZjg3ZWQ0Y2Y3MQUEAw==";
    AzureKeyVaultEdekSerde serde = new AzureKeyVaultEdekSerde();

    @Test
    void testSerializeEdekWithLowercaseHexVersion() {
        byte[] edekBytes = EDEK_BYTES;
        AzureKeyVaultEdek edek = new AzureKeyVaultEdek(KEY_NAME, HEX_VERSION, edekBytes, VAULT_NAME, KEY_TYPE);
        ByteBuffer oversized = ByteBuffer.allocate(1024);
        serde.serialize(edek, oversized);
        oversized.flip();
        assertThat(oversized.get()).isEqualTo((byte) 0); // 16-byte hex version
        assertThat(oversized.get()).isEqualTo((byte) KEY_NAME.length()); // name length
        byte[] nameBytes = new byte[KEY_NAME.length()];
        oversized.get(nameBytes);
        assertThat(new String(nameBytes, StandardCharsets.UTF_8)).isEqualTo(KEY_NAME);
        assertThat(oversized.get()).isEqualTo((byte) VAULT_NAME.length()); // name length
        byte[] vaultNameBytes = new byte[VAULT_NAME.length()];
        oversized.get(vaultNameBytes);
        assertThat(new String(vaultNameBytes, StandardCharsets.UTF_8)).isEqualTo(VAULT_NAME);
        assertThat(oversized.get()).isEqualTo(KEY_TYPE.getId());
        byte[] versionBytes = new byte[16];
        oversized.get(versionBytes);
        // version bytes contains decoded hex of keyVersion
        assertThat(versionBytes).containsExactly(120, -34, -21, -19, 23, 59, 72, -28, -113, 85, -85, -8, 126, -44, -49, 113);
        assertThat(oversized.remaining()).isEqualTo(edekBytes.length);
        byte[] encodedEdek = new byte[edekBytes.length];
        oversized.get(encodedEdek);
        assertThat(encodedEdek).containsExactly(edekBytes);
    }

    @CsvSource({ NON_HEX_VERSION, NON_HEX_VERSION_WITH_UNICODE, UPPER_HEX_VERSION })
    @ParameterizedTest
    void testSerializeEdekWithNonLowercaseHexVersion(String nonHexKeyVersion) {
        AzureKeyVaultEdek edek = new AzureKeyVaultEdek(KEY_NAME, nonHexKeyVersion, EDEK_BYTES, VAULT_NAME, KEY_TYPE);
        ByteBuffer oversized = ByteBuffer.allocate(1024);
        serde.serialize(edek, oversized);
        oversized.flip();
        assertThat(oversized.get()).isEqualTo((byte) 1); // // string version
        byte keyNameLength = oversized.get();
        assertThat(keyNameLength).isEqualTo((byte) KEY_NAME.length()); // name length
        byte[] nameBytes = new byte[KEY_NAME.length()];
        oversized.get(nameBytes);
        assertThat(new String(nameBytes, StandardCharsets.UTF_8)).isEqualTo(KEY_NAME);
        assertThat(oversized.get()).isEqualTo((byte) VAULT_NAME.length()); // name length
        byte[] vaultNameBytes = new byte[VAULT_NAME.length()];
        oversized.get(vaultNameBytes);
        assertThat(new String(vaultNameBytes, StandardCharsets.UTF_8)).isEqualTo(VAULT_NAME);
        assertThat(oversized.get()).isEqualTo(KEY_TYPE.getId());
        int keyVersionLength = nonHexKeyVersion.getBytes(StandardCharsets.UTF_8).length;
        assertThat(oversized.get()).isEqualTo((byte) keyVersionLength);
        byte[] versionBytes = new byte[keyVersionLength];
        oversized.get(versionBytes);
        assertThat(new String(versionBytes, StandardCharsets.UTF_8)).isEqualTo(nonHexKeyVersion);
        assertThat(oversized.remaining()).isEqualTo(EDEK_BYTES.length);
        byte[] encodedEdek = new byte[EDEK_BYTES.length];
        oversized.get(encodedEdek);
        assertThat(encodedEdek).containsExactly(EDEK_BYTES);
    }

    static Stream<Arguments> edekExamples() {
        return Stream.of(Arguments.argumentSet("hex version", LOWER_CASE_HEX_VERSION_EDEK),
                Arguments.argumentSet("non hex version", NON_HEX_VERSION_EDEK),
                Arguments.argumentSet("non hex unicode version", UNICODE_VERSION_EDEK),
                Arguments.argumentSet("hex version with uppercase", UPPERCASE_HEX_VERSION_EDEK));
    }

    @MethodSource("edekExamples")
    @ParameterizedTest
    void serializeDeserializeFidelity(AzureKeyVaultEdek edek) {
        ByteBuffer buff = ByteBuffer.allocate(serde.sizeOf(edek));
        serde.serialize(edek, buff);
        buff.flip();
        AzureKeyVaultEdek deserialized = serde.deserialize(buff);
        assertThat(deserialized).isEqualTo(edek);
    }

    @MethodSource("edekExamples")
    @ParameterizedTest
    void sizeOfAgreesWithSerializedOutputSize(AzureKeyVaultEdek edek) {
        ByteBuffer oversized = ByteBuffer.allocate(1024);
        serde.serialize(edek, oversized);
        oversized.flip();
        int actual = oversized.remaining();
        int expected = serde.sizeOf(edek);
        assertThat(actual).isEqualTo(expected);
    }

    static Stream<Arguments> deserializeFromBinary() {
        return Stream.of(
                Arguments.argumentSet("v0 - lowercase hex version", V0_LOWER_CASE_HEX_VERSION_BINARY, LOWER_CASE_HEX_VERSION_EDEK),
                Arguments.argumentSet("v1 - non hex version", V1_NON_HEX_VERSION_BINARY, NON_HEX_VERSION_EDEK),
                Arguments.argumentSet("v1 - non hex unicode version", V1_UNICODE_VERSION_BINARY, UNICODE_VERSION_EDEK),
                Arguments.argumentSet("v1 - hex version with uppercase", V1_UPPERCASE_HEX_VERSION_BINARY, UPPERCASE_HEX_VERSION_EDEK));
    }

    // we need to be sure we can decode all versions ever emitted
    @MethodSource
    @ParameterizedTest
    void deserializeFromBinary(String base64, AzureKeyVaultEdek expected) {
        try {
            ByteBuffer buff = fromBase64(base64);
            AzureKeyVaultEdek deserialize = serde.deserialize(buff);
            assertThat(deserialize).isNotNull().isEqualTo(expected);
        }
        catch (Exception e) {
            fail("failed to deserialize " + base64);
        }
    }

    @Test
    void unexpectedVersionByteThrows() {
        ByteBuffer buff = ByteBuffer.wrap(new byte[]{ 99 });
        assertThatThrownBy(() -> {
            serde.deserialize(buff);
        }).isInstanceOf(KmsException.class).hasMessage("unknown version byte: 99");
    }

    private static ByteBuffer fromBase64(String src) {
        return ByteBuffer.wrap(Base64.getDecoder().decode(src));
    }

}