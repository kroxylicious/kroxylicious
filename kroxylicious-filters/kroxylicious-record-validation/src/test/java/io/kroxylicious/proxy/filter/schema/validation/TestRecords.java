/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.schema.validation;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.record.DefaultRecord;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.utils.ByteBufferOutputStream;

public class TestRecords {
    public static Record createRecord(String key, String value) {
        return createRecord(key, value, Record.EMPTY_HEADERS);
    }

    public static DefaultRecord createRecord(String key, String value, Header[] headers) {
        ByteBuffer keyBuf = toBufNullable(key);
        ByteBuffer valueBuf = toBufNullable(value);
        try (ByteBufferOutputStream bufferOutputStream = new ByteBufferOutputStream(1000); DataOutputStream dataOutputStream = new DataOutputStream(bufferOutputStream)) {
            DefaultRecord.writeTo(dataOutputStream, 0, 0, keyBuf, valueBuf, headers);
            dataOutputStream.flush();
            bufferOutputStream.flush();
            ByteBuffer buffer = bufferOutputStream.buffer();
            buffer.flip();
            return DefaultRecord.readFrom(buffer, 0, 0, 0, 0L);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static ByteBuffer toBufNullable(String key) {
        if (key == null) {
            return null;
        }
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        return ByteBuffer.wrap(keyBytes);
    }
}
