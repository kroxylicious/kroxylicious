/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.hashicorp.vault;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import io.kroxylicious.kms.service.Serde;

import edu.umd.cs.findbugs.annotations.NonNull;

class VaultEdekSerde implements Serde<VaultEdek> {

    @Override
    public VaultEdek deserialize(@NonNull ByteBuffer buffer) {
        // TODO use varints once approach is agreed.
        short kekRefLength = Serde.getUnsignedByte(buffer);
        if (kekRefLength < 1) {
            throw new IllegalArgumentException("Unexpected key ref length (%d) read deserializing VaultEdek :".formatted(kekRefLength));
        }
        var buf = new byte[kekRefLength];
        buffer.get(buf);
        var keyRef = new String(buf, StandardCharsets.UTF_8);
        int edekLength = buffer.limit() - buffer.position();
        if (edekLength == 0) {
           throw new IllegalArgumentException("unable to deserialize edek as there are zero bytes remaining in the buffer");
        }
        var edek = new byte[edekLength];
        buffer.get(edek);
        return new VaultEdek(keyRef, edek);
    }

    @Override
    public int sizeOf(VaultEdek edek) {
        return edek.kekRef().getBytes(StandardCharsets.UTF_8).length + 1 + edek.edek().length;
    }

    @Override
    public void serialize(VaultEdek edek, @NonNull ByteBuffer buffer) {
        var keyRefBuf = edek.kekRef().getBytes(StandardCharsets.UTF_8);
        if (keyRefBuf.length == 0 || keyRefBuf.length > 255) {
            throw new IllegalArgumentException("Kek ref '%s' is of an unexpected length (%d)".formatted(edek.kekRef(), keyRefBuf.length));
        }
        Serde.putUnsignedByte(buffer, keyRefBuf.length);
        buffer.put(keyRefBuf);
        buffer.put(edek.edek());
    }
}
