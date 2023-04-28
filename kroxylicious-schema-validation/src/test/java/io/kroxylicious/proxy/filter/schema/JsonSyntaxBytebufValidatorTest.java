/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.schema;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.apache.kafka.common.record.DefaultRecord;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.utils.ByteBufferOutputStream;
import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.filter.schema.validation.Result;
import io.kroxylicious.proxy.filter.schema.validation.bytebuf.BytebufValidator;
import io.kroxylicious.proxy.filter.schema.validation.bytebuf.BytebufValidators;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JsonSyntaxBytebufValidatorTest {

    @Test
    public void testSyntacticallyIncorrectRecordInvalidated() {
        Record record = createRecord("a", "b");
        BytebufValidator validator = BytebufValidators.jsonSyntaxValidator(false);
        Result result = validate(record, validator);
        assertFalse(result.valid());
    }

    @Test
    public void testSyntacticallyCorrectRecordValidated() {
        Record record = createRecord("a", "{\"a\":\"a\"}");
        BytebufValidator validator = BytebufValidators.jsonSyntaxValidator(false);
        Result result = validate(record, validator);
        assertTrue(result.valid());
    }

    @Test
    public void testDuplicatedObjectKeyInvalidated() {
        Record record = createRecord("a", "{\"a\":\"a\",\"a\":\"b\"}");
        BytebufValidator validator = BytebufValidators.jsonSyntaxValidator(true);
        Result result = validate(record, validator);
        assertFalse(result.valid());
        assertEquals("value was not syntactically correct JSON: JSON object at $ contained duplicate key: a", result.errorMessage());
    }

    @Test
    public void testDuplicatedObjectKeyInNestedObjectInvalidated() {
        Record record = createRecord("a", "{\"inner\":{\"a\":\"a\",\"a\":\"b\"}}");
        BytebufValidator validator = BytebufValidators.jsonSyntaxValidator(true);
        Result result = validate(record, validator);
        assertFalse(result.valid());
        assertEquals("value was not syntactically correct JSON: JSON object at $.inner contained duplicate key: a", result.errorMessage());
    }

    @Test
    public void testDuplicatedObjectKeyInArrayInvalidated() {
        Record record = createRecord("a", "[{\"a\":\"a\",\"a\":\"b\"}]");
        BytebufValidator validator = BytebufValidators.jsonSyntaxValidator(true);
        Result result = validate(record, validator);
        assertFalse(result.valid());
        assertEquals("value was not syntactically correct JSON: JSON object at $[0] contained duplicate key: a", result.errorMessage());
    }

    @Test
    public void testNonDuplicatedObjectKeyInArrayValidated() {
        Record record = createRecord("a", "[{\"a\":\"a\",\"b\":\"b\"}]");
        BytebufValidator validator = BytebufValidators.jsonSyntaxValidator(true);
        Result result = validate(record, validator);
        assertTrue(result.valid());
    }

    @Test
    public void testArrayWithTwoObjectsWithSameKeysValidated() {
        Record record = createRecord("a", "[{\"a\":\"a\"},{\"a\":\"a\"}]");
        BytebufValidator validator = BytebufValidators.jsonSyntaxValidator(true);
        Result result = validate(record, validator);
        assertTrue(result.valid());
    }

    @Test
    public void testNestedObjectsUsingSameKeysValidated() {
        Record record = createRecord("a", "[{\"a\":{\"a\":\"a\"}}]");
        BytebufValidator validator = BytebufValidators.jsonSyntaxValidator(true);
        Result result = validate(record, validator);
        assertTrue(result.valid());
    }

    @Test
    public void testNestedObjectsWithDuplicateKeysInvalidated() {
        Record record = createRecord("a", "[{\"a\":{\"a\":\"a\",\"a\":\"b\"}}]");
        BytebufValidator validator = BytebufValidators.jsonSyntaxValidator(true);
        Result result = validate(record, validator);
        assertFalse(result.valid());
        assertEquals("value was not syntactically correct JSON: JSON object at $[0].a contained duplicate key: a", result.errorMessage());
    }

    @Test
    public void testDeepObjectsWithDuplicateKeysInvalidated() {
        Record record = createRecord("a", "[[[{\"a\":{\"b\":[1,true,null,{\"duplicate\":1,\"duplicate\":1}]}}]]]]");
        BytebufValidator validator = BytebufValidators.jsonSyntaxValidator(true);
        Result result = validate(record, validator);
        assertFalse(result.valid());
        assertEquals("value was not syntactically correct JSON: JSON object at $[0][0][0].a.b[3] contained duplicate key: duplicate", result.errorMessage());
    }

    @Test
    public void testArrayWithTwoObjectsWithSameKeysAndOtherDataValidated() {
        Record record = createRecord("a", "[{\"a\":\"a\"},2,{\"a\":\"a\"},\"banana\"]");
        BytebufValidator validator = BytebufValidators.jsonSyntaxValidator(true);
        Result result = validate(record, validator);
        assertTrue(result.valid());
    }

    @Test
    public void testNonDuplicatedObjectKeysWithDuplicationValidationEnabled() {
        Record record = createRecord("a", "{\"a\":\"b\",\"c\":\"d\"}");
        BytebufValidator validator = BytebufValidators.jsonSyntaxValidator(true);
        Result result = validate(record, validator);
        assertTrue(result.valid());
    }

    @Test
    public void testDuplicatedObjectKeyValidatedWithDuplicationValidationDisabled() {
        Record record = createRecord("a", "{\"a\":\"a\",\"a\":\"b\"}");
        BytebufValidator validator = BytebufValidators.jsonSyntaxValidator(false);
        Result result = validate(record, validator);
        assertTrue(result.valid());
    }

    @Test
    public void testDifferentObjectsCanHaveSameKeyNames() {
        Record record = createRecord("a", "{\"a\":{\"a\":1},\"b\":{\"a\":2}}");
        BytebufValidator validator = BytebufValidators.jsonSyntaxValidator(true);
        Result result = validate(record, validator);
        assertTrue(result.valid());
    }

    @Test
    public void testTrailingCharactersInvalidated() {
        Record record = createRecord("a", "{\"a\":\"a\"}abc");
        BytebufValidator validator = BytebufValidators.jsonSyntaxValidator(false);
        Result result = validate(record, validator);
        assertFalse(result.valid());
    }

    @Test
    public void testLeadingCharactersInvalidated() {
        Record record = createRecord("a", "abc{\"a\":\"a\"}abc");
        BytebufValidator validator = BytebufValidators.jsonSyntaxValidator(false);
        Result result = validate(record, validator);
        assertFalse(result.valid());
    }

    @Test
    public void testValueValidated() {
        Record record = createRecord("a", "123");
        BytebufValidator validator = BytebufValidators.jsonSyntaxValidator(true);
        Result result = validate(record, validator);
        assertTrue(result.valid());
    }

    @Test
    public void testEmptyStringThrows() {
        Record record = createRecord("a", "");
        BytebufValidator validator = BytebufValidators.jsonSyntaxValidator(false);
        assertThrows(IllegalArgumentException.class, () -> {
            validate(record, validator);
        });
    }

    @Test
    public void testNullValueThrows() {
        Record record = createRecord("a", null);
        BytebufValidator validator = BytebufValidators.jsonSyntaxValidator(false);
        assertThrows(IllegalArgumentException.class, () -> {
            validate(record, validator);
        });
    }

    private static Result validate(Record record, BytebufValidator validator) {
        return validator.validate(record.value(), record.valueSize());
    }

    private Record createRecord(String key, String value) {
        ByteBuffer keyBuf = toBufNullable(key);
        ByteBuffer valueBuf = toBufNullable(value);

        try (ByteBufferOutputStream bufferOutputStream = new ByteBufferOutputStream(1000); DataOutputStream dataOutputStream = new DataOutputStream(bufferOutputStream)) {
            DefaultRecord.writeTo(dataOutputStream, 0, 0, keyBuf, valueBuf, Record.EMPTY_HEADERS);
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