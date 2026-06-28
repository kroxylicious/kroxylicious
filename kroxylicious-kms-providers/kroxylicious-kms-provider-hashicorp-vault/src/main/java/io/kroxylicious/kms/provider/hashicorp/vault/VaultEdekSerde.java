/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.hashicorp.vault;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

import io.kroxylicious.kms.service.Serde;

import static java.lang.Math.toIntExact;
import static org.apache.kafka.common.utils.ByteUtils.readUnsignedVarint;
import static org.apache.kafka.common.utils.ByteUtils.sizeOfUnsignedVarint;
import static org.apache.kafka.common.utils.ByteUtils.writeUnsignedVarint;
import static org.apache.kafka.common.utils.Utils.utf8;
import static org.apache.kafka.common.utils.Utils.utf8Length;

/**
 * Serde for the VaultEdek.
 * <br/>
 * The serialization structure is as follows:
 * <ol>
 *     <li>Protobuf Unsigned varint to hold the number of bytes required to hold the UTF-8 representation of the kekRef.</li>
 *     <li>UTF-8 representation of the kefRef.</li>
 *     <li>Bytes of the edek.</li>
 * </ol>
 * <br/>
 * TODO: consider adding a version byte.
 * @see <a href="https://protobuf.dev/programming-guides/encoding/">Protobuf Encodings</a>
 */
class VaultEdekSerde implements Serde<VaultEdek> {

    private static final Serde<VaultEdek> INSTANCE = new VaultEdekSerde();

    static Serde<VaultEdek> instance() {
        return INSTANCE;
    }

    private VaultEdekSerde() {
    }

    @Override
    public VaultEdek deserialize(ByteBuffer buffer) {
        Objects.requireNonNull(buffer);

        var kekRefLength = toIntExact(readUnsignedVarint(buffer));
        var kekRef = utf8(buffer, kekRefLength);
        buffer.position(buffer.position() + kekRefLength);

        int edekLength = buffer.remaining();
        var edek = new byte[edekLength];
        buffer.get(edek);

        return new VaultEdek(kekRef, edek);
    }

    @Override
    public int sizeOf(VaultEdek edek) {
        Objects.requireNonNull(edek);
        int kekRefLen = utf8Length(edek.kekRef());
        return sizeOfUnsignedVarint(kekRefLen) // varint to store length of kek
                + kekRefLen // n bytes for the utf-8 encoded kek
                + edek.edek().length; // n for the bytes of the edek
    }

    @Override
    public void serialize(VaultEdek edek,
                          ByteBuffer buffer) {
        Objects.requireNonNull(edek);
        Objects.requireNonNull(buffer);
        var keyRefBuf = edek.kekRef().getBytes(StandardCharsets.UTF_8);
        writeUnsignedVarint(keyRefBuf.length, buffer);
        buffer.put(keyRefBuf);
        buffer.put(edek.edek());
    }
}
