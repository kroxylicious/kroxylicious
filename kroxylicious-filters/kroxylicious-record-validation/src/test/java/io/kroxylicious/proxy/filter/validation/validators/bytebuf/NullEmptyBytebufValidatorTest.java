/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.validation.validators.bytebuf;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.common.record.Record;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.kroxylicious.proxy.filter.validation.validators.Result;

import static io.kroxylicious.proxy.filter.validation.validators.bytebuf.BytebufValidators.nullEmptyValidator;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class NullEmptyBytebufValidatorTest {

    @Mock
    BytebufValidator mockValidator;

    @Test
    void nullKeyValid() {
        boolean nullValid = true;

        var record = createMockRecord(null, true);
        BytebufValidator validator = nullEmptyValidator(nullValid, true, mockValidator);
        var result = validator.validate(record.key(), record, true);

        assertThat(result)
                          .succeedsWithin(Duration.ofSeconds(1))
                          .returns(true, Result::valid);

        verifyNoInteractions(mockValidator);
    }

    @Test
    void nullKeyInvalid() {
        boolean nullValid = false;
        BytebufValidator validator = nullEmptyValidator(nullValid, true, mockValidator);

        var record = createMockRecord(null, true);
        var result = validator.validate(record.key(), record, true);

        assertThat(result)
                          .succeedsWithin(Duration.ofSeconds(1))
                          .returns(false, Result::valid);

        verifyNoInteractions(mockValidator);
    }

    @Test
    void emptyKeyValid() {
        boolean emptyValid = true;

        var record = createMockRecord(null, true);
        BytebufValidator validator = nullEmptyValidator(true, emptyValid, mockValidator);
        var result = validator.validate(record.key(), record, true);

        assertThat(result)
                          .succeedsWithin(Duration.ofSeconds(1))
                          .returns(true, Result::valid);

        verifyNoInteractions(mockValidator);
    }

    @Test
    void emptyKeyInvalid() {
        boolean emptyValid = false;
        var record = createMockRecord(new byte[0], true);
        BytebufValidator validator = nullEmptyValidator(true, emptyValid, mockValidator);
        var result = validator.validate(record.key(), record, true);

        assertThat(result)
                          .succeedsWithin(Duration.ofSeconds(1))
                          .returns(false, Result::valid);

        verifyNoInteractions(mockValidator);
    }

    @Test
    void nullValueValid() {
        boolean nullValid = true;

        var record = createMockRecord(null, false);
        BytebufValidator validator = nullEmptyValidator(nullValid, true, mockValidator);
        var result = validator.validate(record.value(), record, false);

        assertThat(result)
                          .succeedsWithin(Duration.ofSeconds(1))
                          .returns(true, Result::valid);

        verifyNoInteractions(mockValidator);
    }

    @Test
    void delegateValid() {
        when(mockValidator.validate(any(), any(), anyBoolean())).thenReturn(Result.VALID_RESULT_STAGE);

        var record = createMockRecord(new byte[1], true);

        BytebufValidator validator = nullEmptyValidator(true, true, mockValidator);

        var result = validator.validate(record.key(), record, true);

        assertThat(result)
                          .succeedsWithin(Duration.ofSeconds(1))
                          .returns(true, Result::valid);

        verify(mockValidator, times(1)).validate(any(ByteBuffer.class), any(Record.class), anyBoolean());
    }

    @Test
    void delegateInvalid() {
        when(mockValidator.validate(any(), any(), anyBoolean())).thenReturn(CompletableFuture.completedFuture(new Result(false, "FAIL")));

        var record = createMockRecord(new byte[1], true);

        BytebufValidator validator = nullEmptyValidator(true, true, mockValidator);

        var result = validator.validate(record.key(), record, true);

        assertThat(result)
                          .succeedsWithin(Duration.ofSeconds(1))
                          .returns(false, Result::valid)
                          .returns("FAIL", Result::errorMessage);

        verify(mockValidator, times(1)).validate(any(ByteBuffer.class), any(Record.class), anyBoolean());
    }

    private Record createMockRecord(byte[] o, boolean isKey) {
        var mock = mock(Record.class);
        var hasField = o != null;
        var fieldSize = o == null ? -1 : o.length;
        var fieldBuf = o == null ? null : ByteBuffer.wrap(o);
        if (isKey) {
            when(mock.hasKey()).thenReturn(hasField);
            when(mock.keySize()).thenReturn(fieldSize);
            when(mock.key()).thenReturn(fieldBuf);
        } else {
            when(mock.hasValue()).thenReturn(hasField);
            when(mock.valueSize()).thenReturn(fieldSize);
            when(mock.value()).thenReturn(fieldBuf);
        }
        return mock;
    }
}
