/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.util;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;
import org.slf4j.spi.LoggingEventBuilder;

import io.netty.channel.ChannelFuture;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class NettyFuturesTest {

    @Mock
    Logger log;

    @Mock
    LoggingEventBuilder loggingEventBuilder;

    @Mock
    ChannelFuture future;

    @Test
    void logFailure_failedFutureWithCause_logsWarning() throws Exception {
        // Given
        var cause = new RuntimeException("channel write failed");
        when(future.isSuccess()).thenReturn(false);
        when(future.cause()).thenReturn(cause);
        when(log.atWarn()).thenReturn(loggingEventBuilder);
        // lenient + explicit types: SLF4J has addKeyValue(String,Object) and addKeyValue(String,Supplier).
        // any(Object.class) targets the Object overload; lenient suppresses strict-mode complaints
        // about the Supplier overload being unstubbed.
        lenient().when(loggingEventBuilder.setCause(any())).thenReturn(loggingEventBuilder);
        lenient().when(loggingEventBuilder.addKeyValue(any(String.class), any(Object.class))).thenReturn(loggingEventBuilder);

        var listener = NettyFutures.logFailure(log, "close");

        // When
        listener.operationComplete(future);

        // Then
        verify(log).atWarn();
        verify(loggingEventBuilder).setCause(cause);
        verify(loggingEventBuilder).addKeyValue("operation", "close");
        verify(loggingEventBuilder).log("Netty channel operation failed");
    }

    @Test
    void logFailure_succeededFuture_doesNotLog() throws Exception {
        // Given
        when(future.isSuccess()).thenReturn(true);

        var listener = NettyFutures.logFailure(log, "close");

        // When
        listener.operationComplete(future);

        // Then
        verifyNoInteractions(log);
    }

    @Test
    void logFailure_failedFutureWithNullCause_doesNotLog() throws Exception {
        // Given
        when(future.isSuccess()).thenReturn(false);
        when(future.cause()).thenReturn(null);

        var listener = NettyFutures.logFailure(log, "close");

        // When
        listener.operationComplete(future);

        // Then
        verifyNoInteractions(log);
    }
}
