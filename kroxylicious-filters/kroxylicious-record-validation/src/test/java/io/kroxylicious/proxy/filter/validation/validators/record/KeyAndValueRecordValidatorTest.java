/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.validation.validators.record;

import java.util.concurrent.CompletableFuture;

import org.apache.kafka.common.record.Record;
import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.filter.validation.validators.Result;
import io.kroxylicious.proxy.filter.validation.validators.bytebuf.BytebufValidator;

import static io.kroxylicious.proxy.filter.validation.validators.record.KeyAndValueRecordValidator.keyAndValueValidator;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

class KeyAndValueRecordValidatorTest {

    public static final String FAIL_MESSAGE = "fail";
    public static final BytebufValidator INVALID = (buffer, record, isKey) -> CompletableFuture.completedFuture(new Result(false, FAIL_MESSAGE));
    public static final BytebufValidator VALID = (buffer, record, isKey) -> Result.VALID_RESULT_STAGE;

    @Test
    void testInvalidKey() {
        RecordValidator recordValidator = keyAndValueValidator(INVALID, VALID);
        assertThat(recordValidator.validate(mock(Record.class)))
                                                                .isCompletedWithValue(new Result(false, "Key was invalid: " + FAIL_MESSAGE));
    }

    @Test
    void testInvalidValue() {
        RecordValidator recordValidator = keyAndValueValidator(VALID, INVALID);
        Result validate = recordValidator.validate(mock(Record.class)).toCompletableFuture().join();
        assertFalse(validate.valid());
        assertEquals("Value was invalid: " + FAIL_MESSAGE, validate.errorMessage());
    }

    @Test
    void testInvalidKeyAndValue() {
        RecordValidator recordValidator = keyAndValueValidator(INVALID, INVALID);
        Result validate = recordValidator.validate(mock(Record.class)).toCompletableFuture().join();
        assertFalse(validate.valid());
        assertEquals("Key was invalid: " + FAIL_MESSAGE, validate.errorMessage());
    }

    @Test
    void testValidKeyAndValue() {
        RecordValidator recordValidator = keyAndValueValidator(VALID, VALID);
        Result validate = recordValidator.validate(mock(Record.class)).toCompletableFuture().join();
        assertTrue(validate.valid());
    }

}
