/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.azure;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HexFormat;
import java.util.Objects;

import javax.annotation.concurrent.ThreadSafe;

import io.kroxylicious.kms.provider.azure.keyvault.SupportedKeyType;
import io.kroxylicious.kms.service.KmsException;
import io.kroxylicious.kms.service.Serde;

import edu.umd.cs.findbugs.annotations.NonNull;

import static org.apache.kafka.common.utils.Utils.utf8;
import static org.apache.kafka.common.utils.Utils.utf8Length;

@ThreadSafe
class AzureKeyVaultEdekSerde implements Serde<AzureKeyVaultEdek> {

    private static final byte KEY_VERSION_ENCODED_AS_HEX_V0 = (byte) 0;
    private static final byte KEY_VERSION_ENCODED_AS_STRING_V1 = (byte) 1;

    @Override
    public int sizeOf(AzureKeyVaultEdek edek) {
        return edek.keyVersion128bit().map(e -> versionAsHexBytesSize(edek)).orElse(versionAsStringSize(edek));
    }

    private static int versionAsStringSize(AzureKeyVaultEdek edek) {
        return 1 // version byte
                + 1 // byte to store length of keyName (max 127 characters)
                + utf8Length(edek.keyName()) // n bytes for the utf-8 encoded keyName
                + 1 // byte to store length of vaultName (max 24 characters)
                + utf8Length(edek.vaultName()) // n bytes for the utf-8 encoded vaultName
                + 1 // byte to store supported key type
                + 1 // byte to store length of keyVersion (max 127 characters)
                + utf8Length(edek.keyVersion())
                + edek.edek().length;
    }

    private static int versionAsHexBytesSize(AzureKeyVaultEdek edek) {
        return 1 // version byte
                + 1 // byte to store length of keyName (max 127 characters)
                + utf8Length(edek.keyName()) // keyName
                + 1 // byte to store length of vaultName (max 24 characters)
                + utf8Length(edek.vaultName()) // n bytes for the utf-8 encoded vaultName
                + 1 // byte to store supported key type
                + 16 // bytes to store hex decoded key version
                + edek.edek().length;
    }

    @Override
    public void serialize(AzureKeyVaultEdek edek, ByteBuffer buffer) {
        Objects.requireNonNull(edek);
        Objects.requireNonNull(buffer);
        var keyRefBuf = edek.keyName().getBytes(StandardCharsets.UTF_8);
        if (keyRefBuf.length > 127) {
            // sanity check, key is validated as ascii and 1-127 characters
            throw new IllegalStateException("ascii key ref of max length 127 somehow encoded to >127 characters");
        }
        if (edek.keyVersion128bit().isPresent()) {
            serializeWithHexVersion(edek, buffer, keyRefBuf);
        }
        else {
            serializedWithStringVersion(edek, buffer, keyRefBuf);
        }
    }

    private static void serializedWithStringVersion(AzureKeyVaultEdek edek, ByteBuffer buffer, byte[] keyRefBytes) {
        buffer.put(KEY_VERSION_ENCODED_AS_STRING_V1);
        buffer.put((byte) keyRefBytes.length);
        buffer.put(keyRefBytes);
        buffer.put((byte) edek.vaultName().length());
        buffer.put(edek.vaultName().getBytes(StandardCharsets.UTF_8));
        buffer.put(edek.supportedKeyType().getId());
        byte[] keyVersionBytes = edek.keyVersion().getBytes(StandardCharsets.UTF_8);
        if (keyVersionBytes.length > 127) {
            // sanity check, very unlikely it somehow blows out even with unicode
            throw new IllegalStateException("somehow a 32 character key version encoded to >127 bytes");
        }
        buffer.put((byte) keyVersionBytes.length);
        buffer.put(keyVersionBytes);
        buffer.put(edek.edek());
    }

    private static void serializeWithHexVersion(AzureKeyVaultEdek edek, ByteBuffer buffer, byte[] keyRefBytes) {
        buffer.put(KEY_VERSION_ENCODED_AS_HEX_V0);
        buffer.put((byte) keyRefBytes.length);
        buffer.put(keyRefBytes);
        buffer.put((byte) edek.vaultName().length());
        buffer.put(edek.vaultName().getBytes(StandardCharsets.UTF_8));
        buffer.put(edek.supportedKeyType().getId());
        buffer.put(edek.keyVersion128bit().orElseThrow());
        buffer.put(edek.edek());
    }

    @Override
    public AzureKeyVaultEdek deserialize(ByteBuffer buffer) {
        Objects.requireNonNull(buffer);
        byte versionByte = buffer.get();
        if (versionByte == KEY_VERSION_ENCODED_AS_HEX_V0) {
            return deserializeHexVersion0(buffer);
        }
        else if (versionByte == KEY_VERSION_ENCODED_AS_STRING_V1) {
            return deserializeStringVersion0(buffer);
        }
        else {
            throw new KmsException("unknown version byte: " + versionByte);
        }
    }

    @NonNull
    private static AzureKeyVaultEdek deserializeStringVersion0(ByteBuffer buffer) {
        byte keyNameLength = buffer.get();
        String keyName = utf8(buffer, keyNameLength);
        buffer.position(buffer.position() + keyNameLength);
        byte vaultNameLength = buffer.get();
        String vaultName = utf8(buffer, vaultNameLength);
        buffer.position(buffer.position() + vaultNameLength);
        byte supportedKeyTypeId = buffer.get();
        byte keyVersionLength = buffer.get();
        String keyVersion = utf8(buffer, keyVersionLength);
        buffer.position(buffer.position() + keyVersionLength);
        int edekLength = buffer.remaining();
        var edek = new byte[edekLength];
        buffer.get(edek);
        return new AzureKeyVaultEdek(keyName, keyVersion, edek, vaultName, toKeyType(supportedKeyTypeId));
    }

    @NonNull
    private static AzureKeyVaultEdek deserializeHexVersion0(ByteBuffer buffer) {
        byte keyNameLength = buffer.get();
        String keyName = utf8(buffer, keyNameLength);
        buffer.position(buffer.position() + keyNameLength);
        byte vaultNameLength = buffer.get();
        String vaultName = utf8(buffer, vaultNameLength);
        buffer.position(buffer.position() + vaultNameLength);
        byte supportedKeyTypeId = buffer.get();
        byte[] keyVersion = new byte[16];
        buffer.get(keyVersion);
        int edekLength = buffer.remaining();
        var edek = new byte[edekLength];
        buffer.get(edek);
        return new AzureKeyVaultEdek(keyName, HexFormat.of().formatHex(keyVersion), edek, vaultName, toKeyType(supportedKeyTypeId));
    }

    private static SupportedKeyType toKeyType(byte supportedKeyTypeId) {
        return SupportedKeyType.fromId(supportedKeyTypeId)
                .orElseThrow(() -> new IllegalStateException("could not decode SupportedKeyType from id " + supportedKeyTypeId));
    }
}
