/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.kafka.fidelity;

import java.nio.ByteBuffer;

import io.kroxylicious.kafka.common.protocol.ByteBufferAccessor;
import io.kroxylicious.kafka.common.protocol.Message;
import io.kroxylicious.kafka.common.protocol.MessageSizeAccumulator;
import io.kroxylicious.kafka.common.protocol.ObjectSerializationCache;

/**
 * Serializes and deserializes {@code io.kroxylicious.kafka.common.protocol.Message} instances to and
 * from raw bytes, using the standard two-pass size-then-write protocol.
 */
public final class KroxyliciousSerdes {

    private KroxyliciousSerdes() {
    }

    /**
     * Serializes a message to bytes at the given version.
     *
     * @param message the message to serialize
     * @param version the protocol version to serialize at
     * @return the serialized bytes
     */
    public static byte[] write(Message message, short version) {
        ObjectSerializationCache cache = new ObjectSerializationCache();
        MessageSizeAccumulator size = new MessageSizeAccumulator();
        message.addSize(size, cache, version);
        ByteBuffer buffer = ByteBuffer.allocate(size.totalSize());
        message.write(new ByteBufferAccessor(buffer), cache, version);
        buffer.flip();
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return bytes;
    }

    /**
     * Deserializes bytes into the given message instance at the given version.
     *
     * @param message the instance to populate
     * @param bytes the bytes to deserialize
     * @param version the protocol version to deserialize at
     * @param <T> the message type
     * @return {@code message}, populated
     */
    public static <T extends Message> T read(T message, byte[] bytes, short version) {
        message.read(new ByteBufferAccessor(ByteBuffer.wrap(bytes)), version);
        return message;
    }
}
