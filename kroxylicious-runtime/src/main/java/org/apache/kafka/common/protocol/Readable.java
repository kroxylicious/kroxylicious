/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package org.apache.kafka.common.protocol;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.protocol.types.RawTaggedField;
import org.apache.kafka.common.record.BaseRecords;
import org.apache.kafka.common.record.MemoryRecords;

public interface Readable {
    byte readByte();
    short readShort();
    int readInt();
    long readLong();
    double readDouble();
    byte[] readArray(int length);
    int readUnsignedVarint();
    ByteBuffer readByteBuffer(int length);
    int readVarint();
    long readVarlong();
    int remaining();

    default String readString(int length) {
        byte[] arr = readArray(length);
        return new String(arr, StandardCharsets.UTF_8);
    }

    default List<RawTaggedField> readUnknownTaggedField(List<RawTaggedField> unknowns, int tag, int size) {
        if (unknowns == null) {
            unknowns = new ArrayList<>();
        }
        byte[] data = readArray(size);
        unknowns.add(new RawTaggedField(tag, data));
        return unknowns;
    }

    default BaseRecords readRecords(int length) {
        if (length < 0) {
            // no records
            return null;
        } else {
            ByteBuffer recordsBuffer = readByteBuffer(length);
            return MemoryRecords.readableRecords(recordsBuffer);
        }
    }

    /**
     * Read a UUID with the most significant digits first.
     */
    default Uuid readUuid() {
        return new Uuid(readLong(), readLong());
    }

    default int readUnsignedShort() {
        return Short.toUnsignedInt(readShort());
    }

    default long readUnsignedInt() {
        return Integer.toUnsignedLong(readInt());
    }
}
