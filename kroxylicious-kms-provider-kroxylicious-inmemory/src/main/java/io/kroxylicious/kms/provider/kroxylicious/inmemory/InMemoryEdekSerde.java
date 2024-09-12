/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.kroxylicious.inmemory;

import java.nio.ByteBuffer;
import java.util.UUID;

import io.kroxylicious.kms.service.Serde;

import edu.umd.cs.findbugs.annotations.NonNull;

class InMemoryEdekSerde implements Serde<InMemoryEdek> {

    private static final InMemoryEdekSerde INSTANCE = new InMemoryEdekSerde();
    private final Serde<UUID> uuidSerde = UUIDSerde.instance();

    private InMemoryEdekSerde() {
    }

    public static Serde<InMemoryEdek> instance() {
        return INSTANCE;
    }

    @Override
    public InMemoryEdek deserialize(@NonNull
    ByteBuffer buffer) {
        short numAuthBits = Serde.getUnsignedByte(buffer);
        var ivLength = Serde.getUnsignedByte(buffer);
        var iv = new byte[ivLength];
        buffer.get(iv);
        UUID keyRef = uuidSerde.deserialize(buffer);
        int edekLength = buffer.limit() - buffer.position();
        var edek = new byte[edekLength];
        buffer.get(edek);
        return new InMemoryEdek(numAuthBits, iv, keyRef, edek);
    }

    @Override
    public int sizeOf(InMemoryEdek inMemoryEdek) {
        return Byte.BYTES // Auth tag: NIST.SP.800-38D §5.2.1.2 suggests max tag length is 128
               + Byte.BYTES // IV length: NIST.SP.800-38D §8.2 certainly doesn't _limit_ IV to 96 bits
               + uuidSerde.sizeOf(inMemoryEdek.kekRef())
               + inMemoryEdek.iv().length
               + inMemoryEdek.edek().length;
    }

    @Override
    public void serialize(InMemoryEdek inMemoryEdek, @NonNull
    ByteBuffer buffer) {
        Serde.putUnsignedByte(buffer, inMemoryEdek.numAuthBits());
        Serde.putUnsignedByte(buffer, inMemoryEdek.iv().length);
        buffer.put(inMemoryEdek.iv());
        uuidSerde.serialize(inMemoryEdek.kekRef(), buffer);
        buffer.put(inMemoryEdek.edek());
    }
}
