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
import io.kroxylicious.proxy.filter.schema.validation.bytebuf.BytebufValidator;
import io.kroxylicious.proxy.filter.schema.validation.bytebuf.BytebufValidators;

import static org.assertj.core.api.Assertions.assertThat;

class ApiCurioSchemaBytebufValidatorTest {

    public static final BytebufValidator APICURIO_SCHEMA_VALIDATOR = BytebufValidators.apicurioSchemaValidator();

    @Test
    void testNullValueAllowed() {
        Result result = validateValue(APICURIO_SCHEMA_VALIDATOR, null, 0, TestRecords.createRecord(null, null));
        assertThat(result.valid()).isTrue();
    }

    @Test
    void testValueHeaderNotLong() {
        Header[] headers = { new RecordHeader("apicurio.value.globalId", new byte[]{ 1, 2, 3, 4, 5, 6, 7 }) };
        ByteBuffer arbitrary = ByteBuffer.allocate(1);
        Result result = validateValue(APICURIO_SCHEMA_VALIDATOR, arbitrary, 0, TestRecords.createRecord(null, null, headers));
        assertThat(result.valid()).isFalse();
        assertThat(result.errorMessage()).contains("header apicurio.value.globalId value is not 8 bytes");
    }

    @Test
    void testValueHeaderLong() {
        Header[] headers = { new RecordHeader("apicurio.value.globalId", new byte[]{ 1, 2, 3, 4, 5, 6, 7, 8 }) };
        ByteBuffer arbitrary = ByteBuffer.allocate(1);
        Result result = validateValue(APICURIO_SCHEMA_VALIDATOR, arbitrary, 0, TestRecords.createRecord(null, null, headers));
        assertThat(result.valid()).isTrue();
    }

    @Test
    void testValueHeaderEmpty() {
        Header[] headers = { new RecordHeader("apicurio.value.globalId", new byte[0]) };
        ByteBuffer arbitrary = ByteBuffer.allocate(1);
        Result result = validateValue(APICURIO_SCHEMA_VALIDATOR, arbitrary, 0, TestRecords.createRecord(null, null, headers));
        assertThat(result.valid()).isFalse();
        assertThat(result.errorMessage()).contains("header apicurio.value.globalId value is not 8 bytes");
    }

    @Test
    void testValueHeaderMissingIsInvalid() {
        ByteBuffer arbitrary = ByteBuffer.allocate(1);
        Result result = validateValue(APICURIO_SCHEMA_VALIDATOR, arbitrary, arbitrary.limit(), TestRecords.createRecord(null, null));
        assertThat(result.valid()).isFalse();
        assertThat(result.errorMessage()).contains("record headers did not contain: apicurio.value.globalId");
    }

    @Test
    void testNullKeyAllowed() {
        Result result = validateKey(APICURIO_SCHEMA_VALIDATOR, null, 0, TestRecords.createRecord(null, null));
        assertThat(result.valid()).isTrue();
    }

    @Test
    void testKeyHeaderMissingIsInvalid() {
        ByteBuffer arbitrary = ByteBuffer.allocate(1);
        Result result = validateKey(APICURIO_SCHEMA_VALIDATOR, arbitrary, arbitrary.limit(), TestRecords.createRecord(null, null));
        assertThat(result.valid()).isFalse();
        assertThat(result.errorMessage()).contains("record headers did not contain: apicurio.key.globalId");
    }

    @Test
    void testKeyHeaderNotLong() {
        Header[] headers = { new RecordHeader("apicurio.key.globalId", new byte[]{ 1, 2, 3, 4, 5, 6, 7 }) };
        ByteBuffer arbitrary = ByteBuffer.allocate(1);
        Result result = validateKey(APICURIO_SCHEMA_VALIDATOR, arbitrary, 0, TestRecords.createRecord(null, null, headers));
        assertThat(result.valid()).isFalse();
        assertThat(result.errorMessage()).contains("header apicurio.key.globalId value is not 8 bytes");
    }

    @Test
    void testKeyHeaderLong() {
        Header[] headers = { new RecordHeader("apicurio.key.globalId", new byte[]{ 1, 2, 3, 4, 5, 6, 7, 8 }) };
        ByteBuffer arbitrary = ByteBuffer.allocate(1);
        Result result = validateKey(APICURIO_SCHEMA_VALIDATOR, arbitrary, 0, TestRecords.createRecord(null, null, headers));
        assertThat(result.valid()).isTrue();
    }

    @Test
    void testKeyHeaderEmpty() {
        Header[] headers = { new RecordHeader("apicurio.key.globalId", new byte[0]) };
        ByteBuffer arbitrary = ByteBuffer.allocate(1);
        Result result = validateKey(APICURIO_SCHEMA_VALIDATOR, arbitrary, 0, TestRecords.createRecord(null, null, headers));
        assertThat(result.valid()).isFalse();
        assertThat(result.errorMessage()).contains("header apicurio.key.globalId value is not 8 bytes");
    }

    private static Result validateValue(BytebufValidator validator, ByteBuffer buffer, int length, Record record) {
        return validate(validator, buffer, length, record, false);
    }

    private static Result validateKey(BytebufValidator validator, ByteBuffer buffer, int length, Record record) {
        return validate(validator, buffer, length, record, true);
    }

    private static Result validate(BytebufValidator validator, ByteBuffer buffer, int length, Record record, boolean isKey) {
        try {
            return validator.validate(buffer, length, record, isKey).toCompletableFuture().get(5, TimeUnit.SECONDS);
        }
        catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

}