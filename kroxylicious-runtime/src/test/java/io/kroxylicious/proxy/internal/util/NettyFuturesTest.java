/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.util;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;
import org.slf4j.spi.LoggingEventBuilder;

import io.netty.channel.ChannelFuture;
import io.netty.util.concurrent.EventExecutor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
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

    @Mock
    EventExecutor executor;

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

    @Test
    void executeOnEventLoop_inEventLoop_runsInline() {
        // Given
        when(executor.inEventLoop()).thenReturn(true);
        var ran = new AtomicBoolean();

        // When
        NettyFutures.executeOnEventLoop(executor, () -> ran.set(true), e -> {
            throw new AssertionError("task must not be rejected");
        });

        // Then
        assertThat(ran).isTrue();
        verify(executor, never()).execute(any());
    }

    @Test
    void executeOnEventLoop_offEventLoop_queuesTask() {
        // Given
        when(executor.inEventLoop()).thenReturn(false);
        Runnable task = () -> {
        };

        // When
        NettyFutures.executeOnEventLoop(executor, task, e -> {
            throw new AssertionError("task must not be rejected");
        });

        // Then
        verify(executor).execute(task);
    }

    @Test
    void executeOnEventLoop_rejected_invokesRejectionHandlerWithoutRunningTask() {
        // Given
        when(executor.inEventLoop()).thenReturn(false);
        var rejection = new RejectedExecutionException("task queue is full");
        doThrow(rejection).when(executor).execute(any());
        var ran = new AtomicBoolean();
        var rejected = new AtomicReference<RejectedExecutionException>();

        // When
        NettyFutures.executeOnEventLoop(executor, () -> ran.set(true), rejected::set);

        // Then
        assertThat(ran).isFalse();
        assertThat(rejected.get()).isSameAs(rejection);
    }

    @Test
    void whenCompleteOnEventLoop_offEventLoop_queuesCompletion() {
        // Given
        when(executor.inEventLoop()).thenReturn(false);
        var stage = new CompletableFuture<String>();
        var result = new AtomicReference<String>();

        // When
        NettyFutures.whenCompleteOnEventLoop(executor, stage, (value, error) -> result.set(value), e -> {
            throw new AssertionError("completion must not be rejected");
        });
        stage.complete("value");

        // Then
        var task = ArgumentCaptor.forClass(Runnable.class);
        verify(executor).execute(task.capture());
        task.getValue().run();
        assertThat(result).hasValue("value");
    }

    @Test
    void whenCompleteOnEventLoop_rejected_invokesRejectionHandlerWithoutRunningCompletion() {
        // Given
        when(executor.inEventLoop()).thenReturn(false);
        var rejection = new RejectedExecutionException("task queue is full");
        doThrow(rejection).when(executor).execute(any());
        var stage = new CompletableFuture<String>();
        var completed = new AtomicBoolean();
        var rejected = new AtomicReference<RejectedExecutionException>();

        // When
        NettyFutures.whenCompleteOnEventLoop(executor, stage, (value, error) -> completed.set(true), rejected::set);
        stage.complete("value");

        // Then
        assertThat(completed).isFalse();
        assertThat(rejected.get()).isSameAs(rejection);
    }
}
