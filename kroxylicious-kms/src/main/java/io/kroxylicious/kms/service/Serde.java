/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.service;

import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ReadOnlyBufferException;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * ByteBuffer-sympathetic serialization and deserialization of objects.
 * @param <T> The type of the object that can be (de)serialized.
 */
public interface Serde<T> {

    /**
     * Write a value in the range 0-255 as a single byte at the buffer's current {@link ByteBuffer#position()}.
     * The buffer's position will be incremented by 1.
     * @param buffer The buffer to write to
     * @param unsignedByte The value, which must be in the range [0, 255], which is not checked by this method.
     * @throws BufferOverflowException If this buffer's current position is not smaller than its limit.
     * @throws ReadOnlyBufferException If this buffer is read-only.
     * @see #getUnsignedByte(ByteBuffer)
     */
    static void putUnsignedByte(
            @NonNull
            ByteBuffer buffer,
            int unsignedByte
    ) {
        buffer.put((byte) (unsignedByte & 0xFF));

    }

    /**
     * Returns the number of bytes required to serialize the given object.
     * @param object The object to be serialized.
     * @return the number of bytes required to serialize the given object.
     */
    int sizeOf(T object);

    /**
     * Serializes the given object to the given buffer.
     * @param object The object to be serialized.
     * @param buffer the buffer to serialize the object to.
     * @throws BufferOverflowException If this buffer's current position is not smaller than its limit.
     * This should never be the case if the {@code buffer} has at least {@link #sizeOf(Object) sizeOf(object)} bytes remaining.
     * @throws ReadOnlyBufferException If this buffer is read-only.
     */
    void serialize(
            T object,
            @NonNull
            ByteBuffer buffer
    );

    /**
     * Read a single byte from the buffer's {@link ByteBuffer#position() position} and return it as a value in the range 0-255.
     * The buffer's position will be incremented by 1.
     * @param buffer The buffer to read from.
     * @return The value, which will be in the range [0, 255].
     * @throws BufferUnderflowException If the buffer's current position is not smaller than its limit.
     * @see #putUnsignedByte(ByteBuffer, int)
     */
    static short getUnsignedByte(
            @NonNull
            ByteBuffer buffer
    ) {
        return (short) (buffer.get() & 0xFF);
    }

    /**
     * Deserialize an instance of {@code T} from the given buffer.
     * @param buffer The buffer.
     * @return The instance, which in general could be null.
     * @throws BufferUnderflowException If the buffer's current position is not smaller than its limit..
     */
    T deserialize(
            @NonNull
            ByteBuffer buffer
    );

}
