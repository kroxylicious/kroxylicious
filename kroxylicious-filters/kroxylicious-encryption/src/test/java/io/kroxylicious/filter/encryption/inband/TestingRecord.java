/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.inband;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.record.TimestampType;

class TestingRecord implements org.apache.kafka.common.record.Record {
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TestingRecord that = (TestingRecord) o;
        return Objects.equals(value, that.value) && Arrays.equals(headers, that.headers);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(value);
        result = 31 * result + Arrays.hashCode(headers);
        return result;
    }

    @Override
    public String toString() {
        return "TestingRecord{" +
                "value=" + value +
                ", headers=" + Arrays.toString(headers) +
                '}';
    }

    private final ByteBuffer value;
    private final Header[] headers;

    TestingRecord(ByteBuffer value, Header... headers) {
        this.value = value;
        this.headers = headers;
    }

    @Override
    public long offset() {
        return 0;
    }

    @Override
    public int sequence() {
        return 0;
    }

    @Override
    public int sizeInBytes() {
        return 0;
    }

    @Override
    public long timestamp() {
        return 0;
    }

    @Override
    public void ensureValid() {

    }

    @Override
    public int keySize() {
        return 0;
    }

    @Override
    public boolean hasKey() {
        return false;
    }

    @Override
    public ByteBuffer key() {
        return null;
    }

    @Override
    public int valueSize() {
        return value.limit();
    }

    @Override
    public boolean hasValue() {
        return value != null;
    }

    @Override
    public ByteBuffer value() {
        return value;
    }

    @Override
    public boolean hasMagic(byte magic) {
        return false;
    }

    @Override
    public boolean isCompressed() {
        return false;
    }

    @Override
    public boolean hasTimestampType(TimestampType timestampType) {
        return false;
    }

    @Override
    public Header[] headers() {
        return headers;
    }
}
