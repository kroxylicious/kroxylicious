/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.schema.validation.bytebuf;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.common.record.Record;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.kroxylicious.proxy.filter.schema.validation.Result;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
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

    @Test
    void chainOfOneSucceeds() {
        // Given
        when(validator1.validate(any(ByteBuffer.class), anyInt(), any(Record.class), anyBoolean())).thenReturn(Result.VALID);

        var chain = BytebufValidators.chainOf(List.of(validator1));

        // When
        var result = chain.validate(BUF, 1, kafkaRecord, false);

        // Then
        assertThat(result)
                .succeedsWithin(Duration.ofSeconds(1))
                .returns(true, Result::valid);

        verify(validator1, times(1)).validate(any(ByteBuffer.class), anyInt(), any(Record.class), anyBoolean());
    }

    @Test
    void chainOfTwoSucceeds() {
        // Given
        when(validator1.validate(any(ByteBuffer.class), anyInt(), any(Record.class), anyBoolean())).thenReturn(Result.VALID);
        when(validator2.validate(any(ByteBuffer.class), anyInt(), any(Record.class), anyBoolean())).thenReturn(Result.VALID);

        var chain = BytebufValidators.chainOf(List.of(validator1, validator2));

        // When
        var result = chain.validate(BUF, 1, kafkaRecord, false);

        // Then
        assertThat(result)
                .succeedsWithin(Duration.ofSeconds(1))
                .returns(true, Result::valid);

        verify(validator1, times(1)).validate(any(ByteBuffer.class), anyInt(), any(Record.class), anyBoolean());
        verify(validator2, times(1)).validate(any(ByteBuffer.class), anyInt(), any(Record.class), anyBoolean());
    }

    @Test
    void firstFailStopsChain() {
        // Given
        when(validator1.validate(any(ByteBuffer.class), anyInt(), any(Record.class), anyBoolean())).thenReturn(CompletableFuture.completedStage(FAIL_RESULT));

        var chain = BytebufValidators.chainOf(List.of(validator1, validator2));

        // When
        var result = chain.validate(BUF, 1, kafkaRecord, false);

        // Then
        assertThat(result)
                .succeedsWithin(Duration.ofSeconds(1))
                .returns(false, Result::valid);

        verify(validator1, times(1)).validate(any(ByteBuffer.class), anyInt(), any(Record.class), anyBoolean());
        verifyNoInteractions(validator2);
    }

    @Test
    void emptyChainYieldsSuccess() {
        // Given
        var chain = BytebufValidators.chainOf(List.of());

        // When
        var result = chain.validate(BUF, 1, kafkaRecord, false);

        // Then
        assertThat(result)
                .succeedsWithin(Duration.ofSeconds(1))
                .returns(true, Result::valid);

    }

}