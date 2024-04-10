/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.schema.validation.bytebuf.apicurio;

import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.record.Record;
import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.filter.schema.validation.Result;
import io.kroxylicious.proxy.filter.schema.validation.TestRecords;

import static org.assertj.core.api.Assertions.assertThat;

class ApiCurioSchemaBytebufValidatorTest {

    @Test
    public void testNullValueAllowed() {
        Result result = validateValue(new ApiCurioSchemaBytebufValidator(), null, 0, TestRecords.createRecord(null, null));
        assertThat(result.valid()).isTrue();
    }

    @Test
    public void testValueHeaderNotLong() {
        Header[] headers = { new RecordHeader("apicurio.value.globalId", new byte[]{ 1, 2, 3, 4, 5, 6, 7 }) };
        ByteBuffer arbitrary = ByteBuffer.allocate(1);
        Result result = validateValue(new ApiCurioSchemaBytebufValidator(), arbitrary, 0, TestRecords.createRecord(null, null, headers));
        assertThat(result.valid()).isFalse();
        assertThat(result.errorMessage()).contains("header apicurio.value.globalId value is not 8 bytes");
    }

    @Test
    public void testValueHeaderLong() {
        Header[] headers = { new RecordHeader("apicurio.value.globalId", new byte[]{ 1, 2, 3, 4, 5, 6, 7, 8 }) };
        ByteBuffer arbitrary = ByteBuffer.allocate(1);
        Result result = validateValue(new ApiCurioSchemaBytebufValidator(), arbitrary, 0, TestRecords.createRecord(null, null, headers));
        assertThat(result.valid()).isTrue();
    }

    @Test
    public void testValueHeaderEmpty() {
        Header[] headers = { new RecordHeader("apicurio.value.globalId", new byte[0]) };
        ByteBuffer arbitrary = ByteBuffer.allocate(1);
        Result result = validateValue(new ApiCurioSchemaBytebufValidator(), arbitrary, 0, TestRecords.createRecord(null, null, headers));
        assertThat(result.valid()).isFalse();
        assertThat(result.errorMessage()).contains("header apicurio.value.globalId value is not 8 bytes");
    }

    @Test
    public void testValueHeaderMissingIsInvalid() {
        ByteBuffer arbitrary = ByteBuffer.allocate(1);
        Result result = validateValue(new ApiCurioSchemaBytebufValidator(), arbitrary, arbitrary.limit(), TestRecords.createRecord(null, null));
        assertThat(result.valid()).isFalse();
        assertThat(result.errorMessage()).contains("record headers did not contain: apicurio.value.globalId");
    }

    @Test
    public void testNullKeyAllowed() {
        Result result = validateKey(new ApiCurioSchemaBytebufValidator(), null, 0, TestRecords.createRecord(null, null));
        assertThat(result.valid()).isTrue();
    }

    @Test
    public void testKeyHeaderMissingIsInvalid() {
        ByteBuffer arbitrary = ByteBuffer.allocate(1);
        Result result = validateKey(new ApiCurioSchemaBytebufValidator(), arbitrary, arbitrary.limit(), TestRecords.createRecord(null, null));
        assertThat(result.valid()).isFalse();
        assertThat(result.errorMessage()).contains("record headers did not contain: apicurio.key.globalId");
    }

    @Test
    public void testKeyHeaderNotLong() {
        Header[] headers = { new RecordHeader("apicurio.key.globalId", new byte[]{ 1, 2, 3, 4, 5, 6, 7 }) };
        ByteBuffer arbitrary = ByteBuffer.allocate(1);
        Result result = validateKey(new ApiCurioSchemaBytebufValidator(), arbitrary, 0, TestRecords.createRecord(null, null, headers));
        assertThat(result.valid()).isFalse();
        assertThat(result.errorMessage()).contains("header apicurio.key.globalId value is not 8 bytes");
    }

    @Test
    public void testKeyHeaderLong() {
        Header[] headers = { new RecordHeader("apicurio.key.globalId", new byte[]{ 1, 2, 3, 4, 5, 6, 7, 8 }) };
        ByteBuffer arbitrary = ByteBuffer.allocate(1);
        Result result = validateKey(new ApiCurioSchemaBytebufValidator(), arbitrary, 0, TestRecords.createRecord(null, null, headers));
        assertThat(result.valid()).isTrue();
    }

    @Test
    public void testKeyHeaderEmpty() {
        Header[] headers = { new RecordHeader("apicurio.key.globalId", new byte[0]) };
        ByteBuffer arbitrary = ByteBuffer.allocate(1);
        Result result = validateKey(new ApiCurioSchemaBytebufValidator(), arbitrary, 0, TestRecords.createRecord(null, null, headers));
        assertThat(result.valid()).isFalse();
        assertThat(result.errorMessage()).contains("header apicurio.key.globalId value is not 8 bytes");
    }

    private static Result validateValue(ApiCurioSchemaBytebufValidator apiCurioSchemaBytebufValidator, ByteBuffer buffer, int length, Record record) {
        return validate(apiCurioSchemaBytebufValidator, buffer, length, record, false);
    }

    private static Result validateKey(ApiCurioSchemaBytebufValidator apiCurioSchemaBytebufValidator, ByteBuffer buffer, int length, Record record) {
        return validate(apiCurioSchemaBytebufValidator, buffer, length, record, true);
    }

    private static Result validate(ApiCurioSchemaBytebufValidator apiCurioSchemaBytebufValidator, ByteBuffer buffer, int length, Record record, boolean isKey) {
        try {
            return apiCurioSchemaBytebufValidator.validate(buffer, length, record, isKey).toCompletableFuture().get(5, TimeUnit.SECONDS);
        }
        catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

}