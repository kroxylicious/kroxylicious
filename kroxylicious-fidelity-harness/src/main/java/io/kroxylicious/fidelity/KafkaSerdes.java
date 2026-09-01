/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.fidelity;

import java.nio.ByteBuffer;

import io.kroxylicious.kafka.common.protocol.ByteBufferAccessor;
import io.kroxylicious.kafka.common.protocol.Message;
import io.kroxylicious.kafka.common.protocol.MessageSizeAccumulator;
import io.kroxylicious.kafka.common.protocol.ObjectSerializationCache;

/**
 * Serializes and deserializes {@code org.apache.kafka.common.protocol.Message} instances to and
 * from raw bytes, using the standard two-pass size-then-write protocol.
 */
public final class KafkaSerdes {

    private KafkaSerdes() {
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
        MessageSizeAccumulator accumulator = new MessageSizeAccumulator();
        message.addSize(accumulator, cache, version);
        ByteBuffer buffer = ByteBuffer.allocate(accumulator.totalSize());
        message.write(new ByteBufferAccessor(buffer), cache, version);
        return MessageSerdesUtil.asByteArray(buffer);
    }

    /**
     * Deserializes bytes into the given message instance at the given version.
     *
     * @param message the instance to populate
     * @param bytes the bytes to deserialize
     * @param version the protocol version to deserialize at
     * @param <T> the message type
     * @return {@code message}, populated, plus how many bytes of {@code bytes} were left unconsumed
     */
    public static <T extends Message> ReadResult<T> read(T message, byte[] bytes, short version) {
        ByteBufferAccessor accessor = new ByteBufferAccessor(ByteBuffer.wrap(bytes));
        try {
            message.read(accessor, version);
            return new ReadResult<>(message, accessor.remaining(), null);
        }
        catch (RuntimeException e) {
            return new ReadResult<>(message, accessor.remaining(), e);
        }
    }
}
