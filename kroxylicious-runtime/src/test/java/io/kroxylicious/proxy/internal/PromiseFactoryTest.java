/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.netty.channel.DefaultEventLoop;
import io.netty.channel.EventLoop;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class PromiseFactoryTest {

    private static final int TIMEOUT = 50;
    private static final TimeUnit TIMEOUT_UNIT = TimeUnit.MILLISECONDS;
    public static final String TEST_LOGGER = "TestLogger";
    private PromiseFactory promiseFactory;
    private DefaultEventLoop eventLoop;

    @BeforeEach
    void setUp() {
        eventLoop = new DefaultEventLoop(Executors.newSingleThreadScheduledExecutor());
        promiseFactory = new PromiseFactory(eventLoop, TIMEOUT, TIMEOUT_UNIT, TEST_LOGGER);
    }

    @AfterEach
    void tearDown() {
        eventLoop.shutdownGracefully(0, 0, TimeUnit.SECONDS);
    }

    @Test
    void shouldCreateNewPromise() {
        // Given

        // When
        final CompletableFuture<Object> promise = promiseFactory.newPromise();

        // Then
        assertThat(promise).isNotNull().isNotDone();
    }

    @Test
    void shouldCreateNewPromiseOnEachInvocation() {
        // Given
        final CompletableFuture<Object> promise = promiseFactory.newPromise();

        // When
        final CompletableFuture<Object> promise2 = promiseFactory.newPromise();

        // Then
        assertThat(promise2).isNotSameAs(promise);
    }

    @SuppressWarnings("unchecked")
    @Test
    void shouldCreatePromiseWithTimeout() {
        // Given
        final EventLoop executorService = mock(EventLoop.class);
        promiseFactory = new PromiseFactory(executorService, TIMEOUT, TIMEOUT_UNIT, TEST_LOGGER);
        when(executorService.schedule(any(Runnable.class), anyLong(), any())).thenReturn(mock(io.netty.util.concurrent.ScheduledFuture.class));

        // When
        final CompletableFuture<Object> promise = promiseFactory.newTimeLimitedPromise(() -> "");

        // Then
        assertThat(promise).isNotNull().isNotDone();
        verify(executorService).schedule(any(Runnable.class), anyLong(), any());
    }

    @Test
    void shouldCancelTimeoutWhenTaskCompletes() {

        final EventLoop executorService = mock(EventLoop.class);
        promiseFactory = new PromiseFactory(executorService, TIMEOUT, TIMEOUT_UNIT, TEST_LOGGER);

        final AtomicReference<io.netty.util.concurrent.ScheduledFuture<?>> timeoutFuture = new AtomicReference<>();
        when(executorService.schedule(any(Runnable.class), anyLong(), any())).thenAnswer(invocationOnMock -> {
            final io.netty.util.concurrent.ScheduledFuture<?> newValue = eventLoop.schedule((Runnable) invocationOnMock.getArgument(0),
                    invocationOnMock.getArgument(1),
                    invocationOnMock.getArgument(2));
            timeoutFuture.set(newValue);
            return newValue;
        });
        when(executorService.isExecutorThread(any())).thenReturn(true);

        final CompletableFuture<Object> promise = promiseFactory.newTimeLimitedPromise(() -> "");

        // When
        promise.complete(null);

        // Then
        assertThat(timeoutFuture).satisfies(atomicRef -> assertThat(atomicRef).hasValueMatching(Objects::nonNull)
                .hasValueSatisfying(scheduledFuture -> Assertions.FUTURE.createAssert(scheduledFuture).isCancelled()));
    }

    @SuppressWarnings("unchecked")
    @Test
    void shouldWrapPromiseWithTimeout() {
        // Given
        final EventLoop executorService = mock(EventLoop.class);
        promiseFactory = new PromiseFactory(executorService, TIMEOUT, TIMEOUT_UNIT, TEST_LOGGER);
        when(executorService.schedule(any(Runnable.class), anyLong(), any())).thenReturn(mock(io.netty.util.concurrent.ScheduledFuture.class));
        final CompletableFuture<Object> incomingFuture = new CompletableFuture<>();

        // When
        final CompletableFuture<Object> promise = promiseFactory.wrapWithTimeLimit(incomingFuture, () -> "");

        // Then
        assertThat(promise).isNotNull().isNotDone();
        assertThat(incomingFuture).isNotDone();
        verify(executorService).schedule(any(Runnable.class), anyLong(), any());
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test
    void shouldCancelTimeoutWhenIncomingFutureCompletes() {
        // Given
        final EventLoop executorService = mock(EventLoop.class);
        promiseFactory = new PromiseFactory(executorService, TIMEOUT, TIMEOUT_UNIT, TEST_LOGGER);
        final io.netty.util.concurrent.ScheduledFuture scheduledFuture = mock(io.netty.util.concurrent.ScheduledFuture.class);
        when(executorService.schedule(any(Runnable.class), anyLong(), any())).thenReturn(scheduledFuture);
        final CompletableFuture<Object> incomingFuture = new CompletableFuture<>();
        final CompletableFuture<Object> promise = promiseFactory.wrapWithTimeLimit(incomingFuture, () -> "");

        // When
        incomingFuture.complete(null);

        // Then
        verify(scheduledFuture).cancel(anyBoolean());
        assertThat(promise).isDone();
    }

    @Test
    void shouldCompleteIncomingFutureExceptionallyWhenTimeoutTriggered() {
        // Given
        final CompletableFuture<Object> incomingFuture = new CompletableFuture<>();
        final Runnable timeoutTask = promiseFactory.timeoutTask(incomingFuture, () -> "Too Slow!");

        // When
        timeoutTask.run();

        // Then
        assertThat(incomingFuture).isDone().isCompletedExceptionally()
                .failsWithin(Duration.ZERO)
                .withThrowableOfType(ExecutionException.class)
                .withCauseInstanceOf(TimeoutException.class)
                .withMessageContaining("Too Slow!");
    }

    @Test
    void shouldCompleteIncomingFutureExceptionallyWhenMessageGeneratorFails() {
        // Given
        final CompletableFuture<Object> incomingFuture = new CompletableFuture<>();
        final Runnable timeoutTask = promiseFactory.timeoutTask(incomingFuture, () -> {
            throw new RuntimeException("Message generator go boom!");
        });

        // When
        timeoutTask.run();

        // Then
        assertThat(incomingFuture).isDone().isCompletedExceptionally()
                .failsWithin(Duration.ZERO)
                .withThrowableOfType(ExecutionException.class)
                .withCauseInstanceOf(TimeoutException.class)
                .withMessageContaining("Promise Timed out");
    }
}
