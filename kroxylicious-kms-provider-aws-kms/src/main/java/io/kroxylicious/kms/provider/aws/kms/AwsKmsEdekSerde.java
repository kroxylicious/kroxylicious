/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.aws.kms;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

import io.kroxylicious.kms.service.Serde;

import edu.umd.cs.findbugs.annotations.NonNull;

import static java.lang.Math.toIntExact;
import static org.apache.kafka.common.utils.ByteUtils.readUnsignedVarint;
import static org.apache.kafka.common.utils.ByteUtils.sizeOfUnsignedVarint;
import static org.apache.kafka.common.utils.ByteUtils.writeUnsignedVarint;
import static org.apache.kafka.common.utils.Utils.utf8;
import static org.apache.kafka.common.utils.Utils.utf8Length;

/**
 * Serde for the AwsKmsEdek.
 * <br/>
 * The serialization structure is as follows:
 * <ol>
 *     <li>version byte (currently always zero)</li>
 *     <li>Protobuf Unsigned varint to hold the number of bytes required to hold the UTF-8 representation of the kekRef.</li>
 *     <li>UTF-8 representation of the kefRef.</li>
 *     <li>Bytes of the edek.</li>
 * </ol>
 * @see <a href="https://protobuf.dev/programming-guides/encoding/">Protobuf Encodings</a>
 */
class AwsKmsEdekSerde implements Serde<AwsKmsEdek> {

    private static final AwsKmsEdekSerde INSTANCE = new AwsKmsEdekSerde();

    public static final byte VERSION_0 = (byte) 0;

    static Serde<AwsKmsEdek> instance() {
        return INSTANCE;
    }

    private AwsKmsEdekSerde() {
    }

    @Override
    public AwsKmsEdek deserialize(@NonNull
    ByteBuffer buffer) {
        Objects.requireNonNull(buffer);

        var version = buffer.get();
        if (version != VERSION_0) {
            throw new IllegalArgumentException("Unexpected version byte, got: %d expecting: %d".formatted(version, VERSION_0));
        }
        var kekRefLength = toIntExact(readUnsignedVarint(buffer));
        var kekRef = utf8(buffer, kekRefLength);
        buffer.position(buffer.position() + kekRefLength);

        int edekLength = buffer.remaining();
        var edek = new byte[edekLength];
        buffer.get(edek);

        return new AwsKmsEdek(kekRef, edek);
    }

    @Override
    public int sizeOf(AwsKmsEdek edek) {
        Objects.requireNonNull(edek);
        int kekRefLen = utf8Length(edek.kekRef());
        return 1 // version byte
               + sizeOfUnsignedVarint(kekRefLen) // varint to store length of kek
               + kekRefLen // n bytes for the utf-8 encoded kek
               + edek.edek().length; // n for the bytes of the edek
    }

    @Override
    public void serialize(AwsKmsEdek edek, @NonNull
    ByteBuffer buffer) {
        Objects.requireNonNull(edek);
        Objects.requireNonNull(buffer);
        var keyRefBuf = edek.kekRef().getBytes(StandardCharsets.UTF_8);
        buffer.put(VERSION_0);
        writeUnsignedVarint(keyRefBuf.length, buffer);
        buffer.put(keyRefBuf);
        buffer.put(edek.edek());
    }
}
