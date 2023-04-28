/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.schema.validation.record;

import org.apache.kafka.common.record.Record;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import io.kroxylicious.proxy.filter.schema.validation.Result;
import io.kroxylicious.proxy.filter.schema.validation.bytebuf.BytebufValidator;

import static io.kroxylicious.proxy.filter.schema.validation.record.KeyAndValueRecordValidator.keyAndValueValidator;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KeyAndValueRecordValidatorTest {

    public static final String FAIL_MESSAGE = "fail";
    public static final BytebufValidator INVALID = (buffer, length, record, isKey) -> new Result(false, FAIL_MESSAGE);
    public static final BytebufValidator VALID = (buffer, length, record, isKey) -> Result.VALID;

    @Test
    void testInvalidKey() {
        RecordValidator recordValidator = keyAndValueValidator(INVALID, VALID);
        Result validate = recordValidator.validate(Mockito.mock(Record.class));
        assertFalse(validate.valid());
        assertEquals("Key was invalid: " + FAIL_MESSAGE, validate.errorMessage());
    }

    @Test
    void testInvalidValue() {
        RecordValidator recordValidator = keyAndValueValidator(VALID, INVALID);
        Result validate = recordValidator.validate(Mockito.mock(Record.class));
        assertFalse(validate.valid());
        assertEquals("Value was invalid: " + FAIL_MESSAGE, validate.errorMessage());
    }

    @Test
    void testInvalidKeyAndValue() {
        RecordValidator recordValidator = keyAndValueValidator(INVALID, INVALID);
        Result validate = recordValidator.validate(Mockito.mock(Record.class));
        assertFalse(validate.valid());
        assertEquals("Key was invalid: " + FAIL_MESSAGE, validate.errorMessage());
    }

    @Test
    void testValidKeyAndValue() {
        RecordValidator recordValidator = keyAndValueValidator(VALID, VALID);
        Result validate = recordValidator.validate(Mockito.mock(Record.class));
        assertTrue(validate.valid());
    }

}