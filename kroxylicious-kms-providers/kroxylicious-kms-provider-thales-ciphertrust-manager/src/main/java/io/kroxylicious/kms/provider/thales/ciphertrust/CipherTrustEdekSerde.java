/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.thales.ciphertrust;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import io.kroxylicious.kms.service.Serde;

import static java.lang.Math.toIntExact;
import static org.apache.kafka.common.utils.ByteUtils.readUnsignedVarint;
import static org.apache.kafka.common.utils.ByteUtils.sizeOfUnsignedVarint;
import static org.apache.kafka.common.utils.ByteUtils.writeUnsignedVarint;
import static org.apache.kafka.common.utils.Utils.utf8;
import static org.apache.kafka.common.utils.Utils.utf8Length;

/**
 * Serializer/deserializer for {@link CipherTrustEdek}.
 * Uses Kafka ByteUtils for efficient varint encoding.
 */
class CipherTrustEdekSerde implements Serde<CipherTrustEdek> {

    private static final Serde<CipherTrustEdek> INSTANCE = new CipherTrustEdekSerde();

    static Serde<CipherTrustEdek> instance() {
        return INSTANCE;
    }

    @Override
    public int sizeOf(CipherTrustEdek edek) {
        int idLength = utf8Length(edek.id());
        int modeLength = utf8Length(edek.mode());

        return sizeOfUnsignedVarint(idLength)
                + idLength
                + sizeOfUnsignedVarint(edek.version())
                + sizeOfUnsignedVarint(modeLength)
                + modeLength
                + sizeOfUnsignedVarint(edek.ciphertext().length)
                + edek.ciphertext().length
                + sizeOfUnsignedVarint(edek.tag().length)
                + edek.tag().length
                + sizeOfUnsignedVarint(edek.iv().length)
                + edek.iv().length;
    }

    @Override
    public void serialize(CipherTrustEdek edek, ByteBuffer buffer) {
        // Serialize id
        byte[] idBytes = edek.id().getBytes(StandardCharsets.UTF_8);
        writeUnsignedVarint(idBytes.length, buffer);
        buffer.put(idBytes);

        // Serialize version
        writeUnsignedVarint(edek.version(), buffer);

        // Serialize mode
        byte[] modeBytes = edek.mode().getBytes(StandardCharsets.UTF_8);
        writeUnsignedVarint(modeBytes.length, buffer);
        buffer.put(modeBytes);

        // Serialize ciphertext
        writeUnsignedVarint(edek.ciphertext().length, buffer);
        buffer.put(edek.ciphertext());

        // Serialize tag
        writeUnsignedVarint(edek.tag().length, buffer);
        buffer.put(edek.tag());

        // Serialize iv
        writeUnsignedVarint(edek.iv().length, buffer);
        buffer.put(edek.iv());
    }

    @Override
    public CipherTrustEdek deserialize(ByteBuffer buffer) {
        // Deserialize id
        int idLength = toIntExact(readUnsignedVarint(buffer));
        String id = utf8(buffer, idLength);
        buffer.position(buffer.position() + idLength);

        // Deserialize version
        int version = toIntExact(readUnsignedVarint(buffer));

        // Deserialize mode
        int modeLength = toIntExact(readUnsignedVarint(buffer));
        String mode = utf8(buffer, modeLength);
        buffer.position(buffer.position() + modeLength);

        // Deserialize ciphertext
        int ciphertextLength = toIntExact(readUnsignedVarint(buffer));
        byte[] ciphertext = new byte[ciphertextLength];
        buffer.get(ciphertext);

        // Deserialize tag
        int tagLength = toIntExact(readUnsignedVarint(buffer));
        byte[] tag = new byte[tagLength];
        buffer.get(tag);

        // Deserialize iv
        int ivLength = toIntExact(readUnsignedVarint(buffer));
        byte[] iv = new byte[ivLength];
        buffer.get(iv);

        return new CipherTrustEdek(id, ciphertext, tag, version, mode, iv);
    }
}
