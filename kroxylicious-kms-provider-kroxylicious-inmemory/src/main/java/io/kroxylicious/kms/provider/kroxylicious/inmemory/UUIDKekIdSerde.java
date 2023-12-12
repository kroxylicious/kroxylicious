/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.kroxylicious.inmemory;

import java.nio.ByteBuffer;
import java.util.UUID;

import io.kroxylicious.kms.service.KekId;
import io.kroxylicious.kms.service.Serde;

import edu.umd.cs.findbugs.annotations.NonNull;

class UUIDKekIdSerde implements Serde<KekId> {

    private static final UUIDKekIdSerde UUID_SERDE = new UUIDKekIdSerde();

    private UUIDKekIdSerde() {
    }

    @Override
    public KekId deserialize(@NonNull ByteBuffer buffer) {
        var msb = buffer.getLong();
        var lsb = buffer.getLong();
        return new UUIDKekId(new UUID(msb, lsb));
    }

    @Override
    public int sizeOf(KekId uuid) {
        return 16;
    }

    @Override
    public void serialize(KekId kekId, @NonNull ByteBuffer buffer) {
        final UUID id = kekId.getId(UUID.class);
        buffer.putLong(id.getMostSignificantBits());
        buffer.putLong(id.getLeastSignificantBits());
    }

    public static Serde<KekId> instance() {
        return UUID_SERDE;
    }
}