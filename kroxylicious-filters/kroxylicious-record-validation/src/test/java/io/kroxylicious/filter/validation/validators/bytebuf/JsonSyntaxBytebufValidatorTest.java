/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.validation.validators.bytebuf;

import java.time.Duration;
import java.util.stream.Stream;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.record.Record;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.junit.jupiter.MockitoExtension;

import io.kroxylicious.filter.validation.validators.Result;

import static io.kroxylicious.test.record.RecordTestUtils.record;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@ExtendWith(MockitoExtension.class)
class JsonSyntaxBytebufValidatorTest {

    private static final String DUPLICATE_ERROR = "value was not syntactically correct JSON: Duplicate field";

    static Stream<Arguments> validJson() {
        return Stream.of(
                Arguments.argumentSet("Syntactically correct record", """
                        {"a":"a"}""", false),
                Arguments.argumentSet("Non-duplicated object key in array validated", """
                        [{"a":"a","b":"b"}]""", true),
                Arguments.argumentSet("Array with two objects with same keys validated", """
                        [{"a":"a"},{"a":"a"}]""", true),
                Arguments.argumentSet("Nested objects using same keys validated", """
                        [{"a":{"a":"a"}}]""", true),
                Arguments.argumentSet("Array with two objects with same keys and other data validated", """
                        [{"a":"a"},2,{"a":"a"},"banana"]""", true),
                Arguments.argumentSet("Non-duplicated object keys with duplication validation enabled", """
                        {"a":"b","c":"d"}""", true),
                Arguments.argumentSet("Duplicated object key validated with duplication validation disabled", """
                        {"a":"a","a":"b"}""", false),
                Arguments.argumentSet("Different objects can have same key names", """
                        {"a":{"a":1},"b":{"a":2}}""", true),
                Arguments.argumentSet("Simple value validated", "123", true, true, null));
    }

    @ParameterizedTest
    @MethodSource()
    void validJson(String json, boolean validateDuplicates) {
        Record jsonRecord = record("a", json);
        BytebufValidator validator = BytebufValidators.jsonSyntaxValidator(validateDuplicates);

        var result = validator.validate(jsonRecord.value(), jsonRecord, false);

        assertThat(result)
                .succeedsWithin(Duration.ofSeconds(1))
                .satisfies(r -> {
                    assertThat(r.valid()).isTrue();
                });
    }

    static Stream<Arguments> invalidJson() {
        return Stream.of(
                Arguments.argumentSet("Duplicated object key invalidated", """
                        {"a":"a","a":"b"}""", true, DUPLICATE_ERROR),
                Arguments.argumentSet("Duplicated object key in nested object invalidated", """
                        {"inner":{"a":"a","a":"b"}}""", true, DUPLICATE_ERROR),
                Arguments.argumentSet("Duplicated object key in array invalidated", """
                        [{"a":"a","a":"b"}]""", true, DUPLICATE_ERROR),
                Arguments.argumentSet("Nested objects with duplicate keys invalidated", """
                        [{"a":{"a":"a","a":"b"}}]""", true, DUPLICATE_ERROR),
                Arguments.argumentSet("Trailing characters invalidated", """
                        {"a":"a"}abc""", false, "value was not syntactically correct JSON: Unrecognized token 'abc'"),
                Arguments.argumentSet("Leading characters invalidated", """
                        abc{"a":"a"}abc""", false, "value was not syntactically correct JSON: Unrecognized token 'abc'"),
                Arguments.argumentSet("Deep objects with duplicate keys invalidated", """
                        [[[{"a":{"b":[1,true,null,{"duplicate":1,"duplicate":1}]}}]]]]""", true, DUPLICATE_ERROR));
    }

    @ParameterizedTest
    @MethodSource()
    void invalidJson(String json, boolean validateDuplicates, String expectedError) {
        Record jsonRecord = record("a", json);
        BytebufValidator validator = BytebufValidators.jsonSyntaxValidator(validateDuplicates);

        var result = validator.validate(jsonRecord.value(), jsonRecord, false);

        assertThat(result)
                .succeedsWithin(Duration.ofSeconds(1))
                .satisfies(r -> {
                    assertThat(r.valid()).isFalse();
                    if (expectedError != null) {
                        assertThat(r.errorMessage()).contains(expectedError);
                    }
                });
    }

    @Test
    void testKeyValidated() {
        Record jsonRecord = record("\"abc\"", "123");
        BytebufValidator validator = BytebufValidators.jsonSyntaxValidator(true);
        var result = validator.validate(jsonRecord.key(), jsonRecord, true);
        assertThat(result)
                .succeedsWithin(Duration.ofSeconds(1))
                .returns(true, Result::valid);
    }

    @Test
    void testEmptyStringThrows() {
        Record jsonRecord = record("a", "");
        BytebufValidator validator = BytebufValidators.jsonSyntaxValidator(false);
        var recBuf = jsonRecord.value();
        assertThatThrownBy(() -> validator.validate(recBuf, jsonRecord, false))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testNullValueThrows() {
        Record jsonRecord = record("a", null, new Header[]{});
        BytebufValidator validator = BytebufValidators.jsonSyntaxValidator(false);
        var recBuf = jsonRecord.value();
        assertThatThrownBy(() -> validator.validate(recBuf, jsonRecord, false))
                .isInstanceOf(IllegalArgumentException.class);
    }
}