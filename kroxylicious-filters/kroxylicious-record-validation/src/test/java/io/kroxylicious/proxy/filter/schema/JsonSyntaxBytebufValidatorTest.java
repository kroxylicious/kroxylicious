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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.common.record.DefaultRecord;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.utils.ByteBufferOutputStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import io.kroxylicious.proxy.filter.schema.validation.Result;
import io.kroxylicious.proxy.filter.schema.validation.bytebuf.BytebufValidator;
import io.kroxylicious.proxy.filter.schema.validation.bytebuf.BytebufValidators;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.verifyNoMoreInteractions;

class JsonSyntaxBytebufValidatorTest {

    @Mock
    BytebufValidator mockValidator;

    @BeforeEach
    public void init() {
        MockitoAnnotations.openMocks(this);
        Mockito.when(mockValidator.validate(any(), anyInt(), any(), anyBoolean())).thenReturn(CompletableFuture.completedFuture(Result.VALID));
    }

    @Test
    void testSyntacticallyIncorrectRecordInvalidated() {
        Record record = createRecord("a", "b");
        BytebufValidator validator = BytebufValidators.jsonSyntaxValidator(false, mockValidator);
        Result result = validate(record, validator);
        assertFalse(result.valid());
        verifyNoMoreInteractions(mockValidator);
    }

    @Test
    void testSyntacticallyCorrectRecordValidated() {
        Record record = createRecord("a", "{\"a\":\"a\"}");
        BytebufValidator validator = BytebufValidators.jsonSyntaxValidator(false, mockValidator);
        Result result = validate(record, validator);
        assertTrue(result.valid());
    }

    @Test
    void testDuplicatedObjectKeyInvalidated() {
        Record record = createRecord("a", "{\"a\":\"a\",\"a\":\"b\"}");
        BytebufValidator validator = BytebufValidators.jsonSyntaxValidator(true, mockValidator);
        Result result = validate(record, validator);
        assertFalse(result.valid());
        assertTrue(result.errorMessage().contains("value was not syntactically correct JSON: Duplicate field"));
        verifyNoMoreInteractions(mockValidator);
    }

    @Test
    void testDuplicatedObjectKeyInNestedObjectInvalidated() {
        Record record = createRecord("a", "{\"inner\":{\"a\":\"a\",\"a\":\"b\"}}");
        BytebufValidator validator = BytebufValidators.jsonSyntaxValidator(true, mockValidator);
        Result result = validate(record, validator);
        assertFalse(result.valid());
        assertTrue(result.errorMessage().contains("value was not syntactically correct JSON: Duplicate field"));
        verifyNoMoreInteractions(mockValidator);
    }

    @Test
    void testDuplicatedObjectKeyInArrayInvalidated() {
        Record record = createRecord("a", "[{\"a\":\"a\",\"a\":\"b\"}]");
        BytebufValidator validator = BytebufValidators.jsonSyntaxValidator(true, mockValidator);
        Result result = validate(record, validator);
        assertFalse(result.valid());
        assertTrue(result.errorMessage().contains("value was not syntactically correct JSON: Duplicate field"));
        verifyNoMoreInteractions(mockValidator);
    }

    @Test
    void testNonDuplicatedObjectKeyInArrayValidated() {
        Record record = createRecord("a", "[{\"a\":\"a\",\"b\":\"b\"}]");
        BytebufValidator validator = BytebufValidators.jsonSyntaxValidator(true, mockValidator);
        Result result = validate(record, validator);
        assertTrue(result.valid());
    }

    @Test
    void testArrayWithTwoObjectsWithSameKeysValidated() {
        Record record = createRecord("a", "[{\"a\":\"a\"},{\"a\":\"a\"}]");
        BytebufValidator validator = BytebufValidators.jsonSyntaxValidator(true, mockValidator);
        Result result = validate(record, validator);
        assertTrue(result.valid());
    }

    @Test
    void testNestedObjectsUsingSameKeysValidated() {
        Record record = createRecord("a", "[{\"a\":{\"a\":\"a\"}}]");
        BytebufValidator validator = BytebufValidators.jsonSyntaxValidator(true, mockValidator);
        Result result = validate(record, validator);
        assertTrue(result.valid());
    }

    @Test
    void testNestedObjectsWithDuplicateKeysInvalidated() {
        Record record = createRecord("a", "[{\"a\":{\"a\":\"a\",\"a\":\"b\"}}]");
        BytebufValidator validator = BytebufValidators.jsonSyntaxValidator(true, mockValidator);
        Result result = validate(record, validator);
        assertFalse(result.valid());
        assertTrue(result.errorMessage().contains("value was not syntactically correct JSON: Duplicate field"));
    }

    @Test
    void testDeepObjectsWithDuplicateKeysInvalidated() {
        Record record = createRecord("a", "[[[{\"a\":{\"b\":[1,true,null,{\"duplicate\":1,\"duplicate\":1}]}}]]]]");
        BytebufValidator validator = BytebufValidators.jsonSyntaxValidator(true, mockValidator);
        Result result = validate(record, validator);
        assertFalse(result.valid());
        assertTrue(result.errorMessage().contains("value was not syntactically correct JSON: Duplicate field"));
    }

    @Test
    void testArrayWithTwoObjectsWithSameKeysAndOtherDataValidated() {
        Record record = createRecord("a", "[{\"a\":\"a\"},2,{\"a\":\"a\"},\"banana\"]");
        BytebufValidator validator = BytebufValidators.jsonSyntaxValidator(true, mockValidator);
        Result result = validate(record, validator);
        assertTrue(result.valid());
    }

    @Test
    void testNonDuplicatedObjectKeysWithDuplicationValidationEnabled() {
        Record record = createRecord("a", "{\"a\":\"b\",\"c\":\"d\"}");
        BytebufValidator validator = BytebufValidators.jsonSyntaxValidator(true, mockValidator);
        Result result = validate(record, validator);
        assertTrue(result.valid());
    }

    @Test
    void testDuplicatedObjectKeyValidatedWithDuplicationValidationDisabled() {
        Record record = createRecord("a", "{\"a\":\"a\",\"a\":\"b\"}");
        BytebufValidator validator = BytebufValidators.jsonSyntaxValidator(false, mockValidator);
        Result result = validate(record, validator);
        assertTrue(result.valid());
    }

    @Test
    void testDifferentObjectsCanHaveSameKeyNames() {
        Record record = createRecord("a", "{\"a\":{\"a\":1},\"b\":{\"a\":2}}");
        BytebufValidator validator = BytebufValidators.jsonSyntaxValidator(true, mockValidator);
        Result result = validate(record, validator);
        assertTrue(result.valid());
    }

    @Test
    void testTrailingCharactersInvalidated() {
        Record record = createRecord("a", "{\"a\":\"a\"}abc");
        BytebufValidator validator = BytebufValidators.jsonSyntaxValidator(false, mockValidator);
        Result result = validate(record, validator);
        assertFalse(result.valid());
    }

    @Test
    void testLeadingCharactersInvalidated() {
        Record record = createRecord("a", "abc{\"a\":\"a\"}abc");
        BytebufValidator validator = BytebufValidators.jsonSyntaxValidator(false, mockValidator);
        Result result = validate(record, validator);
        assertFalse(result.valid());
    }

    @Test
    void testValueValidated() {
        Record record = createRecord("a", "123");
        BytebufValidator validator = BytebufValidators.jsonSyntaxValidator(true, mockValidator);
        Result result = validate(record, validator);
        assertTrue(result.valid());
    }

    @Test
    void testKeyValidated() throws ExecutionException, InterruptedException, TimeoutException {
        Record record = createRecord("\"abc\"", "123");
        BytebufValidator validator = BytebufValidators.jsonSyntaxValidator(true, mockValidator);
        Result result = validator.validate(record.key(), record.keySize(), record, true).toCompletableFuture().get(5, TimeUnit.SECONDS);
        assertTrue(result.valid());
    }

    @Test
    void testEmptyStringThrows() {
        Record record = createRecord("a", "");
        BytebufValidator validator = BytebufValidators.jsonSyntaxValidator(false, mockValidator);
        assertThrows(IllegalArgumentException.class, () -> {
            validate(record, validator);
        });
        verifyNoMoreInteractions(mockValidator);
    }

    @Test
    void testNullValueThrows() {
        Record record = createRecord("a", null);
        BytebufValidator validator = BytebufValidators.jsonSyntaxValidator(false, mockValidator);
        assertThrows(IllegalArgumentException.class, () -> {
            validate(record, validator);
        });
        verifyNoMoreInteractions(mockValidator);
    }

    private static Result validate(Record record, BytebufValidator validator) {
        try {
            return validator.validate(record.value(), record.valueSize(), record, false).toCompletableFuture().get(5, TimeUnit.SECONDS);
        }
        catch (ExecutionException | InterruptedException | TimeoutException e) {
            throw new RuntimeException(e);
        }
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
