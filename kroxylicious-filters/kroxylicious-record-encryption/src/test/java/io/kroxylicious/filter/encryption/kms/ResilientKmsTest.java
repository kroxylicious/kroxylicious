/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.kms;

import java.time.Duration;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.crypto.SecretKey;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import io.kroxylicious.kms.service.DekPair;
import io.kroxylicious.kms.service.Kms;
import io.kroxylicious.kms.service.Serde;
import io.kroxylicious.kms.service.UnknownAliasException;
import io.kroxylicious.kms.service.UnknownKeyException;

import edu.umd.cs.findbugs.annotations.NonNull;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class ResilientKmsTest {

    private static final long RESULT = 7L;
    private static final long DELAY = 10;
    public static final SecretKey SECRET_KEY = mock(SecretKey.class);
    public static final DekPair<Long> DEK_PAIR = new DekPair<>(2L, SECRET_KEY);

    @Test
    void testResolveAlias() {
        // given
        Kms<Long, Long> kms = Mockito.mock(Kms.class);
        when(kms.resolveAlias("abc")).thenReturn(completedFuture(RESULT));
        BackoffStrategy strategy = Mockito.mock(BackoffStrategy.class);
        when(strategy.getDelay(anyInt())).thenReturn(Duration.ofMillis(DELAY));
        ScheduledExecutorService mockExecutor = getMockExecutor();
        Kms<Long, Long> resilientKms = ResilientKms.wrap(kms, mockExecutor, strategy, 3);

        // when
        CompletionStage<Long> kekId = resilientKms.resolveAlias("abc");

        // then
        assertThat(kekId).succeedsWithin(5, TimeUnit.SECONDS).isEqualTo(RESULT);
        verify(mockExecutor, times(1)).schedule(any(Runnable.class), eq(DELAY), eq(TimeUnit.MILLISECONDS));
    }

    @Test
    void testGenerateDek() {
        // given
        Kms<Long, Long> kms = Mockito.mock(Kms.class);
        when(kms.generateDekPair(1L)).thenReturn(completedFuture(DEK_PAIR));
        BackoffStrategy strategy = Mockito.mock(BackoffStrategy.class);
        when(strategy.getDelay(anyInt())).thenReturn(Duration.ofMillis(DELAY));
        ScheduledExecutorService mockExecutor = getMockExecutor();
        Kms<Long, Long> resilientKms = ResilientKms.wrap(kms, mockExecutor, strategy, 3);

        // when
        CompletionStage<DekPair<Long>> dekPair = resilientKms.generateDekPair(1L);

        // then
        assertThat(dekPair).succeedsWithin(5, TimeUnit.SECONDS).isEqualTo(DEK_PAIR);
        verify(mockExecutor, times(1)).schedule(any(Runnable.class), eq(DELAY), eq(TimeUnit.MILLISECONDS));
    }

    @Test
    void testDecryptEdek() {
        // given
        Kms<Long, Long> kms = Mockito.mock(Kms.class);
        when(kms.decryptEdek(1L)).thenReturn(completedFuture(SECRET_KEY));
        BackoffStrategy strategy = Mockito.mock(BackoffStrategy.class);
        when(strategy.getDelay(anyInt())).thenReturn(Duration.ofMillis(DELAY));
        ScheduledExecutorService mockExecutor = getMockExecutor();
        Kms<Long, Long> resilientKms = ResilientKms.wrap(kms, mockExecutor, strategy, 3);

        // when
        CompletionStage<SecretKey> dek = resilientKms.decryptEdek(1L);

        // then
        assertThat(dek).succeedsWithin(5, TimeUnit.SECONDS).isEqualTo(SECRET_KEY);
        verify(mockExecutor, times(1)).schedule(any(Runnable.class), eq(DELAY), eq(TimeUnit.MILLISECONDS));
    }

    @Test
    void testResolveAliasWithNoDelayDoesNotScheduleDelayedWork() {
        // given
        Kms<Long, Long> kms = Mockito.mock(Kms.class);
        when(kms.resolveAlias("abc")).thenReturn(completedFuture(RESULT));
        BackoffStrategy strategy = Mockito.mock(BackoffStrategy.class);
        when(strategy.getDelay(anyInt())).thenReturn(Duration.ZERO);
        ScheduledExecutorService mockExecutor = getMockExecutor();
        Kms<Long, Long> resilientKms = ResilientKms.wrap(kms, mockExecutor, strategy, 3);

        // when
        CompletionStage<Long> kekId = resilientKms.resolveAlias("abc");

        // then
        assertThat(kekId).succeedsWithin(5, TimeUnit.SECONDS).isEqualTo(RESULT);
        // we do not need to schedule any work since the delay calculated by the backoff strategy is 0
        verifyNoInteractions(mockExecutor);
    }

    @Test
    void testGenerateDekWithNoDelayDoesNotScheduleDelayedWork() {
        // given
        Kms<Long, Long> kms = Mockito.mock(Kms.class);
        when(kms.generateDekPair(1L)).thenReturn(completedFuture(DEK_PAIR));
        BackoffStrategy strategy = Mockito.mock(BackoffStrategy.class);
        when(strategy.getDelay(anyInt())).thenReturn(Duration.ZERO);
        ScheduledExecutorService mockExecutor = getMockExecutor();
        Kms<Long, Long> resilientKms = ResilientKms.wrap(kms, mockExecutor, strategy, 3);

        // when
        CompletionStage<DekPair<Long>> dekPair = resilientKms.generateDekPair(1L);

        // then
        assertThat(dekPair).succeedsWithin(5, TimeUnit.SECONDS).isEqualTo(DEK_PAIR);
        verifyNoInteractions(mockExecutor);
    }

    @Test
    void testDecryptEdekWithNoDelayDoesNotScheduleDelayedWork() {
        // given
        Kms<Long, Long> kms = Mockito.mock(Kms.class);
        when(kms.decryptEdek(1L)).thenReturn(completedFuture(SECRET_KEY));
        BackoffStrategy strategy = Mockito.mock(BackoffStrategy.class);
        when(strategy.getDelay(anyInt())).thenReturn(Duration.ZERO);
        ScheduledExecutorService mockExecutor = getMockExecutor();
        Kms<Long, Long> resilientKms = ResilientKms.wrap(kms, mockExecutor, strategy, 3);

        // when
        CompletionStage<SecretKey> dek = resilientKms.decryptEdek(1L);

        // then
        assertThat(dek).succeedsWithin(5, TimeUnit.SECONDS).isEqualTo(SECRET_KEY);
        verifyNoInteractions(mockExecutor);
    }

    @Test
    void testResolveAliasRetries() {
        // given
        Kms<Long, Long> kms = Mockito.mock(Kms.class);
        when(kms.resolveAlias("abc")).thenReturn(failedFuture(new RuntimeException("BOOM! test exception")), completedFuture(RESULT));
        BackoffStrategy strategy = Mockito.mock(BackoffStrategy.class);
        when(strategy.getDelay(anyInt())).thenReturn(Duration.ofMillis(DELAY));
        ScheduledExecutorService mockExecutor = getMockExecutor();
        Kms<Long, Long> resilientKms = ResilientKms.wrap(kms, mockExecutor, strategy, 3);

        // when
        CompletionStage<Long> kekId = resilientKms.resolveAlias("abc");

        // then
        assertThat(kekId).succeedsWithin(5, TimeUnit.SECONDS).isEqualTo(RESULT);
        verify(mockExecutor, times(2)).schedule(any(Runnable.class), eq(DELAY), eq(TimeUnit.MILLISECONDS));
    }

    @Test
    void testResolveAliasDoesNotRetryUnknownAlias() {
        // given
        Kms<Long, Long> kms = Mockito.mock(Kms.class);
        UnknownAliasException cause = new UnknownAliasException("unknown alias");
        when(kms.resolveAlias("abc")).thenReturn(failedFuture(cause), completedFuture(RESULT));
        BackoffStrategy strategy = Mockito.mock(BackoffStrategy.class);
        when(strategy.getDelay(anyInt())).thenReturn(Duration.ofMillis(DELAY));
        ScheduledExecutorService mockExecutor = getMockExecutor();
        Kms<Long, Long> resilientKms = ResilientKms.wrap(kms, mockExecutor, strategy, 3);

        // when
        CompletionStage<Long> kekId = resilientKms.resolveAlias("abc");

        // then
        assertThat(kekId).failsWithin(5, TimeUnit.SECONDS).withThrowableThat().isInstanceOf(ExecutionException.class).withCause(cause);
        verify(mockExecutor, times(1)).schedule(any(Runnable.class), eq(DELAY), eq(TimeUnit.MILLISECONDS));
    }

    @Test
    void testResolveAliasDoesNotRetryUnknownAliasWrappedInCompletionException() {
        // given
        Kms<Long, Long> kms = Mockito.mock(Kms.class);
        UnknownAliasException unknownAlias = new UnknownAliasException("unknown alias");
        Throwable cause = new CompletionException("fail", unknownAlias);
        when(kms.resolveAlias("abc")).thenReturn(failedFuture(cause), completedFuture(RESULT));
        BackoffStrategy strategy = Mockito.mock(BackoffStrategy.class);
        when(strategy.getDelay(anyInt())).thenReturn(Duration.ofMillis(DELAY));
        ScheduledExecutorService mockExecutor = getMockExecutor();
        Kms<Long, Long> resilientKms = ResilientKms.wrap(kms, mockExecutor, strategy, 3);

        // when
        CompletionStage<Long> kekId = resilientKms.resolveAlias("abc");

        // then
        assertThat(kekId).failsWithin(5, TimeUnit.SECONDS).withThrowableThat().isInstanceOf(ExecutionException.class).withCause(unknownAlias);
        verify(mockExecutor, times(1)).schedule(any(Runnable.class), eq(DELAY), eq(TimeUnit.MILLISECONDS));
    }

    @Test
    void testGenerateDekRetries() {
        // given
        Kms<Long, Long> kms = Mockito.mock(Kms.class);
        when(kms.generateDekPair(1L)).thenReturn(
                failedFuture(new RuntimeException("BOOM! test exception")),
                completedFuture(DEK_PAIR)
        );
        BackoffStrategy strategy = Mockito.mock(BackoffStrategy.class);
        when(strategy.getDelay(anyInt())).thenReturn(Duration.ofMillis(DELAY));
        ScheduledExecutorService mockExecutor = getMockExecutor();
        Kms<Long, Long> resilientKms = ResilientKms.wrap(kms, mockExecutor, strategy, 3);

        // when
        CompletionStage<DekPair<Long>> dekPair = resilientKms.generateDekPair(1L);

        // then
        assertThat(dekPair).succeedsWithin(5, TimeUnit.SECONDS).isEqualTo(DEK_PAIR);
        verify(mockExecutor, times(2)).schedule(any(Runnable.class), eq(DELAY), eq(TimeUnit.MILLISECONDS));
    }

    @Test
    void testGenerateDekDoesNotRetryUnknownKey() {
        // given
        Kms<Long, Long> kms = Mockito.mock(Kms.class);
        when(kms.generateDekPair(1L)).thenReturn(
                failedFuture(new UnknownKeyException("unknown key")),
                completedFuture(DEK_PAIR)
        );
        BackoffStrategy strategy = Mockito.mock(BackoffStrategy.class);
        when(strategy.getDelay(anyInt())).thenReturn(Duration.ofMillis(DELAY));
        ScheduledExecutorService mockExecutor = getMockExecutor();
        Kms<Long, Long> resilientKms = ResilientKms.wrap(kms, mockExecutor, strategy, 3);

        // when
        CompletionStage<DekPair<Long>> dekPair = resilientKms.generateDekPair(1L);

        // then
        assertThat(dekPair).failsWithin(5, TimeUnit.SECONDS).withThrowableThat().withMessageContaining("unknown key");
        verify(mockExecutor, times(1)).schedule(any(Runnable.class), eq(DELAY), eq(TimeUnit.MILLISECONDS));
    }

    @Test
    void testDecryptEdekRetries() {
        // given
        Kms<Long, Long> kms = Mockito.mock(Kms.class);
        when(kms.decryptEdek(1L)).thenReturn(failedFuture(new RuntimeException("BOOM! test exception")), completedFuture(SECRET_KEY));
        BackoffStrategy strategy = Mockito.mock(BackoffStrategy.class);
        when(strategy.getDelay(anyInt())).thenReturn(Duration.ofMillis(DELAY));
        ScheduledExecutorService mockExecutor = getMockExecutor();
        Kms<Long, Long> resilientKms = ResilientKms.wrap(kms, mockExecutor, strategy, 3);

        // when
        CompletionStage<SecretKey> dek = resilientKms.decryptEdek(1L);

        // then
        assertThat(dek).succeedsWithin(5, TimeUnit.SECONDS).isEqualTo(SECRET_KEY);
        verify(mockExecutor, times(2)).schedule(any(Runnable.class), eq(DELAY), eq(TimeUnit.MILLISECONDS));
    }

    @Test
    void testDecryptEdekDoesNotRetryUnknownKey() {
        // given
        Kms<Long, Long> kms = Mockito.mock(Kms.class);
        when(kms.decryptEdek(1L)).thenReturn(failedFuture(new UnknownKeyException("unknown key")), completedFuture(SECRET_KEY));
        BackoffStrategy strategy = Mockito.mock(BackoffStrategy.class);
        when(strategy.getDelay(anyInt())).thenReturn(Duration.ofMillis(DELAY));
        ScheduledExecutorService mockExecutor = getMockExecutor();
        Kms<Long, Long> resilientKms = ResilientKms.wrap(kms, mockExecutor, strategy, 3);

        // when
        CompletionStage<SecretKey> dek = resilientKms.decryptEdek(1L);

        // then
        assertThat(dek).failsWithin(5, TimeUnit.SECONDS).withThrowableThat().withMessageContaining("unknown key");
        verify(mockExecutor, times(1)).schedule(any(Runnable.class), eq(DELAY), eq(TimeUnit.MILLISECONDS));
    }

    @Test
    void testResolveAliasFailsAfterNRetries() {
        // given
        Kms<Long, Long> kms = Mockito.mock(Kms.class);
        when(kms.resolveAlias("abc")).thenReturn(failedFuture(new RuntimeException("BOOM! test exception")));
        BackoffStrategy strategy = Mockito.mock(BackoffStrategy.class);
        when(strategy.getDelay(anyInt())).thenReturn(Duration.ofMillis(DELAY));
        int retries = 3;
        ScheduledExecutorService mockExecutor = getMockExecutor();
        Kms<Long, Long> resilientKms = ResilientKms.wrap(kms, mockExecutor, strategy, retries);

        // when
        CompletionStage<Long> kekId = resilientKms.resolveAlias("abc");

        // then
        assertThat(kekId).failsWithin(5, TimeUnit.SECONDS).withThrowableThat().withMessageContaining("resolveAlias failed after " + retries + " attempts");
        Mockito.verify(kms, times(retries)).resolveAlias("abc");
        verify(mockExecutor, times(retries)).schedule(any(Runnable.class), eq(DELAY), eq(TimeUnit.MILLISECONDS));
    }

    @Test
    void testGenerateDekFailsAfterNRetries() {
        // given
        Kms<Long, Long> kms = Mockito.mock(Kms.class);
        when(kms.generateDekPair(1L)).thenReturn(failedFuture(new RuntimeException("BOOM! test exception")));
        BackoffStrategy strategy = Mockito.mock(BackoffStrategy.class);
        when(strategy.getDelay(anyInt())).thenReturn(Duration.ofMillis(DELAY));
        ScheduledExecutorService mockExecutor = getMockExecutor();
        int retries = 3;
        Kms<Long, Long> resilientKms = ResilientKms.wrap(kms, mockExecutor, strategy, retries);

        // when
        CompletionStage<DekPair<Long>> dekPair = resilientKms.generateDekPair(1L);

        // then
        assertThat(dekPair).failsWithin(5, TimeUnit.SECONDS).withThrowableThat().withMessageContaining("generateDekPair failed after " + retries + " attempts");
        Mockito.verify(kms, times(retries)).generateDekPair(1L);
        verify(mockExecutor, times(3)).schedule(any(Runnable.class), eq(DELAY), eq(TimeUnit.MILLISECONDS));
    }

    @Test
    void testDecryptEdekFailsAfterNRetries() {
        // given
        Kms<Long, Long> kms = Mockito.mock(Kms.class);
        when(kms.decryptEdek(1L)).thenReturn(failedFuture(new RuntimeException("BOOM! test exception")));
        BackoffStrategy strategy = Mockito.mock(BackoffStrategy.class);
        when(strategy.getDelay(anyInt())).thenReturn(Duration.ofMillis(DELAY));
        ScheduledExecutorService mockExecutor = getMockExecutor();
        int retries = 3;
        Kms<Long, Long> resilientKms = ResilientKms.wrap(kms, mockExecutor, strategy, retries);

        // when
        CompletionStage<SecretKey> dek = resilientKms.decryptEdek(1L);

        // then
        assertThat(dek).failsWithin(5, TimeUnit.SECONDS).withThrowableThat().withMessageContaining("decryptEdek failed after " + retries + " attempts");
        verify(mockExecutor, times(3)).schedule(any(Runnable.class), eq(DELAY), eq(TimeUnit.MILLISECONDS));
    }

    @Test
    void testGetEdekSerde() {
        // given
        Kms<Long, Long> kms = Mockito.mock(Kms.class);
        Serde mockSerde = mock(Serde.class);
        when(kms.edekSerde()).thenReturn(mockSerde);
        Kms<Long, Long> resilientKms = ResilientKms.wrap(kms, getMockExecutor(), Mockito.mock(BackoffStrategy.class), 3);

        // when
        Serde<?> serde = resilientKms.edekSerde();

        // then
        assertThat(serde).isSameAs(mockSerde);
    }

    @Test
    void testExecutorNotNullable() {
        Kms<Long, Long> kms = Mockito.mock(Kms.class);
        Serde mockSerde = mock(Serde.class);
        when(kms.edekSerde()).thenReturn(mockSerde);
        BackoffStrategy backoffStrategy = mock(BackoffStrategy.class);
        assertThatThrownBy(() -> ResilientKms.wrap(kms, null, backoffStrategy, 3))
                                                                                  .isInstanceOf(NullPointerException.class);
    }

    @Test
    void testInnerKmsNotNullable() {
        BackoffStrategy backoffStrategy = mock(BackoffStrategy.class);
        ScheduledExecutorService executor = getMockExecutor();
        assertThatThrownBy(() -> ResilientKms.wrap(null, executor, backoffStrategy, 3))
                                                                                       .isInstanceOf(NullPointerException.class);
    }

    @Test
    void testBackoffStrategyNotNullable() {
        Kms<Long, Long> kms = Mockito.mock(Kms.class);
        Serde mockSerde = mock(Serde.class);
        when(kms.edekSerde()).thenReturn(mockSerde);
        ScheduledExecutorService executor = getMockExecutor();
        assertThatThrownBy(() -> ResilientKms.wrap(kms, executor, null, 3))
                                                                           .isInstanceOf(NullPointerException.class);
    }

    @NonNull
    private static ScheduledExecutorService getMockExecutor() {
        ScheduledExecutorService mockExecutor = Mockito.mock(ScheduledExecutorService.class);
        when(mockExecutor.schedule(any(Runnable.class), anyLong(), any())).thenAnswer(invocationOnMock -> {
            Runnable argument = invocationOnMock.getArgument(0);
            argument.run();
            return null;
        });
        return mockExecutor;
    }

}
