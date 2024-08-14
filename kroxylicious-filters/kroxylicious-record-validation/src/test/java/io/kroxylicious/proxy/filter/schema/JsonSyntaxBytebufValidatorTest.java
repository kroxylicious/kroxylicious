/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.schema;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.common.record.Record;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import io.kroxylicious.proxy.filter.schema.validation.Result;
import io.kroxylicious.proxy.filter.schema.validation.bytebuf.BytebufValidator;
import io.kroxylicious.proxy.filter.schema.validation.bytebuf.BytebufValidators;

import static io.kroxylicious.test.record.RecordTestUtils.record;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
class JsonSyntaxBytebufValidatorTest {

    @Test
    void testSyntacticallyIncorrectRecordInvalidated() {
        Record record = record("a", "b");
        BytebufValidator validator = BytebufValidators.jsonSyntaxValidator(false);
        Result result = validate(record, validator);
        assertFalse(result.valid());
    }

    @Test
    void testSyntacticallyCorrectRecordValidated() {
        Record record = record("a", "{\"a\":\"a\"}");
        BytebufValidator validator = BytebufValidators.jsonSyntaxValidator(false);
        Result result = validate(record, validator);
        assertTrue(result.valid());
    }

    @Test
    void testDuplicatedObjectKeyInvalidated() {
        Record record = record("a", "{\"a\":\"a\",\"a\":\"b\"}");
        BytebufValidator validator = BytebufValidators.jsonSyntaxValidator(true);
        Result result = validate(record, validator);
        assertFalse(result.valid());
        assertTrue(result.errorMessage().contains("value was not syntactically correct JSON: Duplicate field"));
    }

    @Test
    void testDuplicatedObjectKeyInNestedObjectInvalidated() {
        Record record = record("a", "{\"inner\":{\"a\":\"a\",\"a\":\"b\"}}");
        BytebufValidator validator = BytebufValidators.jsonSyntaxValidator(true);
        Result result = validate(record, validator);
        assertFalse(result.valid());
        assertTrue(result.errorMessage().contains("value was not syntactically correct JSON: Duplicate field"));
    }

    @Test
    void testDuplicatedObjectKeyInArrayInvalidated() {
        Record record = record("a", "[{\"a\":\"a\",\"a\":\"b\"}]");
        BytebufValidator validator = BytebufValidators.jsonSyntaxValidator(true);
        Result result = validate(record, validator);
        assertFalse(result.valid());
        assertTrue(result.errorMessage().contains("value was not syntactically correct JSON: Duplicate field"));
    }

    @Test
    void testNonDuplicatedObjectKeyInArrayValidated() {
        Record record = record("a", "[{\"a\":\"a\",\"b\":\"b\"}]");
        BytebufValidator validator = BytebufValidators.jsonSyntaxValidator(true);
        Result result = validate(record, validator);
        assertTrue(result.valid());
    }

    @Test
    void testArrayWithTwoObjectsWithSameKeysValidated() {
        Record record = record("a", "[{\"a\":\"a\"},{\"a\":\"a\"}]");
        BytebufValidator validator = BytebufValidators.jsonSyntaxValidator(true);
        Result result = validate(record, validator);
        assertTrue(result.valid());
    }

    @Test
    void testNestedObjectsUsingSameKeysValidated() {
        Record record = record("a", "[{\"a\":{\"a\":\"a\"}}]");
        BytebufValidator validator = BytebufValidators.jsonSyntaxValidator(true);
        Result result = validate(record, validator);
        assertTrue(result.valid());
    }

    @Test
    void testNestedObjectsWithDuplicateKeysInvalidated() {
        Record record = record("a", "[{\"a\":{\"a\":\"a\",\"a\":\"b\"}}]");
        BytebufValidator validator = BytebufValidators.jsonSyntaxValidator(true);
        Result result = validate(record, validator);
        assertFalse(result.valid());
        assertTrue(result.errorMessage().contains("value was not syntactically correct JSON: Duplicate field"));
    }

    @Test
    void testDeepObjectsWithDuplicateKeysInvalidated() {
        Record record = record("a", "[[[{\"a\":{\"b\":[1,true,null,{\"duplicate\":1,\"duplicate\":1}]}}]]]]");
        BytebufValidator validator = BytebufValidators.jsonSyntaxValidator(true);
        Result result = validate(record, validator);
        assertFalse(result.valid());
        assertTrue(result.errorMessage().contains("value was not syntactically correct JSON: Duplicate field"));
    }

    @Test
    void testArrayWithTwoObjectsWithSameKeysAndOtherDataValidated() {
        Record record = record("a", "[{\"a\":\"a\"},2,{\"a\":\"a\"},\"banana\"]");
        BytebufValidator validator = BytebufValidators.jsonSyntaxValidator(true);
        Result result = validate(record, validator);
        assertTrue(result.valid());
    }

    @Test
    void testNonDuplicatedObjectKeysWithDuplicationValidationEnabled() {
        Record record = record("a", "{\"a\":\"b\",\"c\":\"d\"}");
        BytebufValidator validator = BytebufValidators.jsonSyntaxValidator(true);
        Result result = validate(record, validator);
        assertTrue(result.valid());
    }

    @Test
    void testDuplicatedObjectKeyValidatedWithDuplicationValidationDisabled() {
        Record record = record("a", "{\"a\":\"a\",\"a\":\"b\"}");
        BytebufValidator validator = BytebufValidators.jsonSyntaxValidator(false);
        Result result = validate(record, validator);
        assertTrue(result.valid());
    }

    @Test
    void testDifferentObjectsCanHaveSameKeyNames() {
        Record record = record("a", "{\"a\":{\"a\":1},\"b\":{\"a\":2}}");
        BytebufValidator validator = BytebufValidators.jsonSyntaxValidator(true);
        Result result = validate(record, validator);
        assertTrue(result.valid());
    }

    @Test
    void testTrailingCharactersInvalidated() {
        Record record = record("a", "{\"a\":\"a\"}abc");
        BytebufValidator validator = BytebufValidators.jsonSyntaxValidator(false);
        Result result = validate(record, validator);
        assertFalse(result.valid());
    }

    @Test
    void testLeadingCharactersInvalidated() {
        Record record = record("a", "abc{\"a\":\"a\"}abc");
        BytebufValidator validator = BytebufValidators.jsonSyntaxValidator(false);
        Result result = validate(record, validator);
        assertFalse(result.valid());
    }

    @Test
    void testValueValidated() {
        Record record = record("a", "123");
        BytebufValidator validator = BytebufValidators.jsonSyntaxValidator(true);
        Result result = validate(record, validator);
        assertTrue(result.valid());
    }

    @Test
    void testKeyValidated() throws ExecutionException, InterruptedException, TimeoutException {
        Record record = record("\"abc\"", "123");
        BytebufValidator validator = BytebufValidators.jsonSyntaxValidator(true);
        Result result = validator.validate(record.key(), record, true).toCompletableFuture().get(5, TimeUnit.SECONDS);
        assertTrue(result.valid());
    }

    @Test
    void testEmptyStringThrows() {
        Record record = record("a", "");
        BytebufValidator validator = BytebufValidators.jsonSyntaxValidator(false);
        assertThrows(IllegalArgumentException.class, () -> {
            validate(record, validator);
        });
    }

    @Test
    void testNullValueThrows() {
        Record record = record("a", (String) null);
        BytebufValidator validator = BytebufValidators.jsonSyntaxValidator(false);
        assertThrows(IllegalArgumentException.class, () -> validate(record, validator));
    }

    private static Result validate(Record record, BytebufValidator validator) {
        try {
            return validator.validate(record.value(), record, false).toCompletableFuture().get(5, TimeUnit.SECONDS);
        }
        catch (ExecutionException | InterruptedException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

}
