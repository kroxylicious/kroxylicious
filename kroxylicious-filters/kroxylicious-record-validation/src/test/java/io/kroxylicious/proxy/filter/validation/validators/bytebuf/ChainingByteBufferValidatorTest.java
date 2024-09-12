/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.validation.validators.bytebuf;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.common.record.Record;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.kroxylicious.proxy.filter.validation.validators.Result;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ChainingByteBufferValidatorTest {

    private static final ByteBuffer BUF = ByteBuffer.wrap("bytes".getBytes(StandardCharsets.UTF_8));
    private static final Result FAIL_RESULT = new Result(false, "mocked fail");

    @Mock
    private final Record kafkaRecord = mock(Record.class);

    @Mock
    private BytebufValidator validator1 = mock(BytebufValidator.class);
    @Mock
    private BytebufValidator validator2 = mock(BytebufValidator.class);

    @Captor
    private ArgumentCaptor<ByteBuffer> byteBufferCaptor;

    @Test
    void chainOfOneSucceeds() {
        // Given
        when(validator1.validate(any(ByteBuffer.class), any(Record.class), anyBoolean())).thenReturn(Result.VALID_RESULT_STAGE);

        var chain = BytebufValidators.chainOf(List.of(validator1));

        // When
        var result = chain.validate(BUF, kafkaRecord, false);

        // Then
        assertThat(result)
                          .succeedsWithin(Duration.ofSeconds(1))
                          .returns(true, Result::valid);
    }

    @Test
    void chainOfTwoSucceeds() {
        // Given
        when(validator1.validate(any(ByteBuffer.class), any(Record.class), anyBoolean())).thenReturn(Result.VALID_RESULT_STAGE);
        when(validator2.validate(any(ByteBuffer.class), any(Record.class), anyBoolean())).thenReturn(Result.VALID_RESULT_STAGE);

        var chain = BytebufValidators.chainOf(List.of(validator1, validator2));

        // When
        var result = chain.validate(BUF, kafkaRecord, false);

        // Then
        assertThat(result)
                          .succeedsWithin(Duration.ofSeconds(1))
                          .returns(true, Result::valid);
    }

    @Test
    void validatorsInvokedInExpectedSequenceAndCorrectNumberOfTimes() {
        // Given
        when(validator1.validate(any(ByteBuffer.class), any(Record.class), anyBoolean())).thenReturn(Result.VALID_RESULT_STAGE);
        when(validator2.validate(any(ByteBuffer.class), any(Record.class), anyBoolean())).thenReturn(Result.VALID_RESULT_STAGE);

        var chain = BytebufValidators.chainOf(List.of(validator1, validator2));

        // When
        chain.validate(BUF, kafkaRecord, false);

        // Then
        var inOrder = inOrder(validator1, validator2);
        inOrder.verify(validator1, times(1)).validate(any(ByteBuffer.class), any(Record.class), anyBoolean());
        inOrder.verify(validator2, times(1)).validate(any(ByteBuffer.class), any(Record.class), anyBoolean());
    }

    @Test
    void firstFailStopsChain() {
        // Given
        when(validator1.validate(any(ByteBuffer.class), any(Record.class), anyBoolean())).thenReturn(CompletableFuture.completedStage(FAIL_RESULT));

        var chain = BytebufValidators.chainOf(List.of(validator1, validator2));

        // When
        var result = chain.validate(BUF, kafkaRecord, false);

        // Then
        assertThat(result)
                          .succeedsWithin(Duration.ofSeconds(1))
                          .returns(false, Result::valid);

        verify(validator1, times(1)).validate(any(ByteBuffer.class), any(Record.class), anyBoolean());
        verifyNoInteractions(validator2);
    }

    @Test
    void secondFailReported() {
        // Given
        when(validator1.validate(any(ByteBuffer.class), any(Record.class), anyBoolean())).thenReturn(Result.VALID_RESULT_STAGE);
        when(validator2.validate(any(ByteBuffer.class), any(Record.class), anyBoolean())).thenReturn(CompletableFuture.completedStage(FAIL_RESULT));

        var chain = BytebufValidators.chainOf(List.of(validator1, validator2));

        // When
        var result = chain.validate(BUF, kafkaRecord, false);

        // Then
        assertThat(result)
                          .succeedsWithin(Duration.ofSeconds(1))
                          .returns(false, Result::valid);

    }

    @Test
    void emptyChainYieldsSuccess() {
        // Given
        var chain = BytebufValidators.chainOf(List.of());

        // When
        var result = chain.validate(BUF, kafkaRecord, false);

        // Then
        assertThat(result)
                          .succeedsWithin(Duration.ofSeconds(1))
                          .returns(true, Result::valid);

    }

    @Test
    void passesReadOnlyBuffersToValidators() {
        // Given
        when(validator1.validate(byteBufferCaptor.capture(), any(Record.class), anyBoolean())).thenReturn(Result.VALID_RESULT_STAGE);

        var chain = BytebufValidators.chainOf(List.of(validator1));

        // When
        chain.validate(BUF, kafkaRecord, false);

        // Then
        assertThat(byteBufferCaptor.getAllValues())
                                                   .singleElement()
                                                   .returns(true, ByteBuffer::isReadOnly);

        verify(validator1, times(1)).validate(any(ByteBuffer.class), any(Record.class), anyBoolean());
    }

}
