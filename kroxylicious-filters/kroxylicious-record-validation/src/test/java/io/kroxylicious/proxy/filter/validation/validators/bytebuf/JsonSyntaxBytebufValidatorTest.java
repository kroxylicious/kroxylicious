/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.validation.validators.bytebuf;

import java.time.Duration;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.record.Record;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import io.kroxylicious.proxy.filter.validation.validators.Result;

import static io.kroxylicious.test.record.RecordTestUtils.record;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@ExtendWith(MockitoExtension.class)
class JsonSyntaxBytebufValidatorTest {

    @Test
    void testSyntacticallyIncorrectRecordInvalidated() {
        Record record = record("a", "b");
        BytebufValidator validator = BytebufValidators.jsonSyntaxValidator(false);
        var result = validator.validate(record.value(), record, false);
        assertThat(result)
                          .succeedsWithin(Duration.ofSeconds(1))
                          .returns(false, Result::valid);
    }

    @Test
    void testSyntacticallyCorrectRecordValidated() {
        Record record = record("a", "{\"a\":\"a\"}");
        BytebufValidator validator = BytebufValidators.jsonSyntaxValidator(false);
        var result = validator.validate(record.value(), record, false);
        assertThat(result)
                          .succeedsWithin(Duration.ofSeconds(1))
                          .returns(true, Result::valid);
    }

    @Test
    void testDuplicatedObjectKeyInvalidated() {
        Record record = record("a", """
                {"a":"a","a":"b"}""");
        BytebufValidator validator = BytebufValidators.jsonSyntaxValidator(true);
        var result = validator.validate(record.value(), record, false);
        assertThat(result)
                          .succeedsWithin(Duration.ofSeconds(1))
                          .satisfies(r -> {
                              assertThat(r.valid()).isFalse();
                              assertThat(r.errorMessage()).contains("value was not syntactically correct JSON: Duplicate field");
                          });
    }

    @Test
    void testDuplicatedObjectKeyInNestedObjectInvalidated() {
        Record record = record("a", """
                {"inner":{"a":"a","a":"b"}}""");
        BytebufValidator validator = BytebufValidators.jsonSyntaxValidator(true);
        var result = validator.validate(record.value(), record, false);
        assertThat(result)
                          .succeedsWithin(Duration.ofSeconds(1))
                          .satisfies(r -> {
                              assertThat(r.valid()).isFalse();
                              assertThat(r.errorMessage()).contains("value was not syntactically correct JSON: Duplicate field");
                          });
    }

    @Test
    void testDuplicatedObjectKeyInArrayInvalidated() {
        Record record = record("a", """
                [{"a":"a","a":"b"}]""");
        BytebufValidator validator = BytebufValidators.jsonSyntaxValidator(true);
        var result = validator.validate(record.value(), record, false);
        assertThat(result)
                          .succeedsWithin(Duration.ofSeconds(1))
                          .satisfies(r -> {
                              assertThat(r.valid()).isFalse();
                              assertThat(r.errorMessage()).contains("value was not syntactically correct JSON: Duplicate field");
                          });
    }

    @Test
    void testNonDuplicatedObjectKeyInArrayValidated() {
        Record record = record("a", """
                [{"a":"a","b":"b"}]""");
        BytebufValidator validator = BytebufValidators.jsonSyntaxValidator(true);
        var result = validator.validate(record.value(), record, false);
        assertThat(result)
                          .succeedsWithin(Duration.ofSeconds(1))
                          .returns(true, Result::valid);
    }

    @Test
    void testArrayWithTwoObjectsWithSameKeysValidated() {
        Record record = record("a", """
                [{"a":"a"},{"a":"a"}]""");
        BytebufValidator validator = BytebufValidators.jsonSyntaxValidator(true);
        var result = validator.validate(record.value(), record, false);
        assertThat(result)
                          .succeedsWithin(Duration.ofSeconds(1))
                          .returns(true, Result::valid);
    }

    @Test
    void testNestedObjectsUsingSameKeysValidated() {
        Record record = record("a", """
                [{"a":{"a":"a"}}] """);
        BytebufValidator validator = BytebufValidators.jsonSyntaxValidator(true);
        var result = validator.validate(record.value(), record, false);
        assertThat(result)
                          .succeedsWithin(Duration.ofSeconds(1))
                          .returns(true, Result::valid);
    }

    @Test
    void testNestedObjectsWithDuplicateKeysInvalidated() {
        Record record = record("a", """
                [{"a":{"a":"a","a":"b"}}]""");
        BytebufValidator validator = BytebufValidators.jsonSyntaxValidator(true);
        var result = validator.validate(record.value(), record, false);
        assertThat(result)
                          .succeedsWithin(Duration.ofSeconds(1))
                          .satisfies(r -> {
                              assertThat(r.valid()).isFalse();
                              assertThat(r.errorMessage()).contains("value was not syntactically correct JSON: Duplicate field");
                          });
    }

    @Test
    void testDeepObjectsWithDuplicateKeysInvalidated() {
        Record record = record("a", """
                [[[{"a":{"b":[1,true,null,{"duplicate":1,"duplicate":1}]}}]]]]""");
        BytebufValidator validator = BytebufValidators.jsonSyntaxValidator(true);
        var result = validator.validate(record.value(), record, false);
        assertThat(result)
                          .succeedsWithin(Duration.ofSeconds(1))
                          .satisfies(r -> {
                              assertThat(r.valid()).isFalse();
                              assertThat(r.errorMessage()).contains("value was not syntactically correct JSON: Duplicate field");
                          });
    }

    @Test
    void testArrayWithTwoObjectsWithSameKeysAndOtherDataValidated() {
        Record record = record("a", """
                [{"a":"a"},2,{"a":"a"},"banana"]""");
        BytebufValidator validator = BytebufValidators.jsonSyntaxValidator(true);
        var result = validator.validate(record.value(), record, false);
        assertThat(result)
                          .succeedsWithin(Duration.ofSeconds(1))
                          .returns(true, Result::valid);
    }

    @Test
    void testNonDuplicatedObjectKeysWithDuplicationValidationEnabled() {
        Record record = record("a", """
                {"a":"b","c":"d"}""");
        BytebufValidator validator = BytebufValidators.jsonSyntaxValidator(true);
        var result = validator.validate(record.value(), record, false);
        assertThat(result)
                          .succeedsWithin(Duration.ofSeconds(1))
                          .returns(true, Result::valid);
    }

    @Test
    void testDuplicatedObjectKeyValidatedWithDuplicationValidationDisabled() {
        Record record = record("a", """
                {"a":"a","a":"b"}""");
        BytebufValidator validator = BytebufValidators.jsonSyntaxValidator(false);
        var result = validator.validate(record.value(), record, false);
        assertThat(result)
                          .succeedsWithin(Duration.ofSeconds(1))
                          .returns(true, Result::valid);
    }

    @Test
    void testDifferentObjectsCanHaveSameKeyNames() {
        Record record = record("a", """
                {"a":{"a":1},"b":{"a":2}}""");
        BytebufValidator validator = BytebufValidators.jsonSyntaxValidator(true);
        var result = validator.validate(record.value(), record, false);
        assertThat(result)
                          .succeedsWithin(Duration.ofSeconds(1))
                          .returns(true, Result::valid);
    }

    @Test
    void testTrailingCharactersInvalidated() {
        Record record = record("a", """
                {"a":"a"}abc""");
        BytebufValidator validator = BytebufValidators.jsonSyntaxValidator(false);
        var result = validator.validate(record.value(), record, false);
        assertThat(result)
                          .succeedsWithin(Duration.ofSeconds(1))
                          .returns(false, Result::valid);
    }

    @Test
    void testLeadingCharactersInvalidated() {
        Record record = record("a", """
                abc{"a":"a"}abc""");
        BytebufValidator validator = BytebufValidators.jsonSyntaxValidator(false);
        var result1 = validator.validate(record.value(), record, false);
        assertThat(result1)
                           .succeedsWithin(Duration.ofSeconds(1))
                           .returns(false, Result::valid);
    }

    @Test
    void testValueValidated() {
        Record record = record("a", "123");
        BytebufValidator validator = BytebufValidators.jsonSyntaxValidator(true);
        var result = validator.validate(record.value(), record, false);
        assertThat(result)
                          .succeedsWithin(Duration.ofSeconds(1))
                          .returns(true, Result::valid);
    }

    @Test
    void testKeyValidated() {
        Record record = record("\"abc\"", "123");
        BytebufValidator validator = BytebufValidators.jsonSyntaxValidator(true);
        var result = validator.validate(record.key(), record, true);
        assertThat(result)
                          .succeedsWithin(Duration.ofSeconds(1))
                          .returns(true, Result::valid);
    }

    @Test
    void testEmptyStringThrows() {
        Record record = record("a", "");
        BytebufValidator validator = BytebufValidators.jsonSyntaxValidator(false);
        var recBuf = record.value();
        assertThatThrownBy(() -> {
            validator.validate(recBuf, record, false);
        }).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testNullValueThrows() {
        Record record = record("a", null, new Header[]{});
        BytebufValidator validator = BytebufValidators.jsonSyntaxValidator(false);
        var recBuf = record.value();
        assertThatThrownBy(() -> {
            validator.validate(recBuf, record, false);
        }).isInstanceOf(IllegalArgumentException.class);
    }

}
