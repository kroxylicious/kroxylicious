/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.encryption;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Base64;
import java.util.Objects;

import io.kroxylicious.kms.service.Serde;

import edu.umd.cs.findbugs.annotations.NonNull;

public record BytesEdek(byte[] edek) {

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BytesEdek bytesEdek = (BytesEdek) o;
        return Objects.deepEquals(edek, bytesEdek.edek);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(edek);
    }

    @Override
    public String toString() {
        return "BytesEdek{"
               +
               "edek="
               + Base64.getEncoder().encodeToString(edek)
               +
               '}';
    }

    static Serde<BytesEdek> getSerde() {
        return new Serde<>() {
            @Override
            public int sizeOf(BytesEdek object) {
                return object.edek.length;
            }

            @Override
            public void serialize(
                    BytesEdek object,
                    @NonNull
            ByteBuffer buffer
            ) {
                throw new UnsupportedOperationException("serialize not supported");
            }

            @Override
            public BytesEdek deserialize(
                    @NonNull
            ByteBuffer buffer
            ) {
                byte[] bytes = new byte[buffer.remaining()];
                buffer.get(bytes);
                return new BytesEdek(bytes);
            }
        };
    }
}
