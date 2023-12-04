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

class UUIDSerde implements Serde<KekId<UUID>> {

    private static final UUIDSerde UUID_SERDE = new UUIDSerde();

    private UUIDSerde() {
    }

    @Override
    public KekId<UUID> deserialize(@NonNull ByteBuffer buffer) {
        var msb = buffer.getLong();
        var lsb = buffer.getLong();
        return new UuidKekId(new UUID(msb, lsb));
    }

    @Override
    public int sizeOf(KekId<UUID> uuid) {
        return 16;
    }

    @Override
    public void serialize(KekId<UUID> uuidKekId, @NonNull ByteBuffer buffer) {
        final UUID id = uuidKekId.getId();
        buffer.putLong(id.getMostSignificantBits());
        buffer.putLong(id.getLeastSignificantBits());
    }

    public static Serde<UUID> instance() {
        return UUID_SERDE;
    }
}