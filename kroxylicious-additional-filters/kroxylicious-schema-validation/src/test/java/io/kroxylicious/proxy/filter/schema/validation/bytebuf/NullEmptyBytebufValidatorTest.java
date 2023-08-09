/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.schema.validation.bytebuf;

import java.nio.ByteBuffer;

import org.apache.kafka.common.record.Record;
import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.filter.schema.validation.Result;

import static io.kroxylicious.proxy.filter.schema.validation.bytebuf.BytebufValidators.nullEmptyValidator;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class NullEmptyBytebufValidatorTest {

    private final Record record = mock(Record.class);

    @Test
    void testNullValid() {
        BytebufValidator mockValidator = mock(BytebufValidator.class);
        boolean nullValid = true;
        BytebufValidator validator = nullEmptyValidator(nullValid, true, mockValidator);
        Result validate = validator.validate(null, 0, record, true);
        assertTrue(validate.valid());
        verifyNoInteractions(mockValidator);
    }

    @Test
    void testNullInvalid() {
        BytebufValidator mockValidator = mock(BytebufValidator.class);
        boolean nullValid = false;
        BytebufValidator validator = nullEmptyValidator(nullValid, true, mockValidator);
        Result validate = validator.validate(null, 0, record, true);
        assertFalse(validate.valid());
        verifyNoInteractions(mockValidator);
    }

    @Test
    void testEmptyValid() {
        BytebufValidator mockValidator = mock(BytebufValidator.class);
        boolean emptyValid = true;
        BytebufValidator validator = nullEmptyValidator(true, emptyValid, mockValidator);
        Result validate = validator.validate(ByteBuffer.wrap(new byte[0]), 0, record, true);
        assertTrue(validate.valid());
        verifyNoInteractions(mockValidator);
    }

    @Test
    void testEmptyInvalid() {
        BytebufValidator mockValidator = mock(BytebufValidator.class);
        boolean emptyValid = false;
        BytebufValidator validator = nullEmptyValidator(true, emptyValid, mockValidator);
        Result validate = validator.validate(ByteBuffer.wrap(new byte[0]), 0, record, true);
        assertFalse(validate.valid());
        verifyNoInteractions(mockValidator);
    }

    @Test
    void testDelegation() {
        BytebufValidator mockValidator = mock(BytebufValidator.class);
        when(mockValidator.validate(any(), anyInt(), any(), anyBoolean())).thenReturn(new Result(false, "FAIL"));
        BytebufValidator validator = nullEmptyValidator(true, true, mockValidator);
        ByteBuffer buffer = ByteBuffer.wrap(new byte[1]);
        int length = 1;
        Result validate = validator.validate(buffer, length, record, true);
        assertFalse(validate.valid());
        assertEquals("FAIL", validate.errorMessage());
        verify(mockValidator).validate(buffer, length, record, true);
    }

}
