/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.util;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test cases for {@link ActivationToken}.
 *
 * This class tests the thread-safe state management and exactly-once semantics
 * of the ActivationToken class, ensuring proper lifecycle management and
 * race condition prevention.
 */
class ActivationTokenTest {

    private AtomicInteger counter;
    private ActivationToken token;

    @BeforeEach
    void setUp() {
        counter = new AtomicInteger(0);
        token = new ActivationToken(counter);
    }

    @Test
    @DisplayName("acquire() increments counter exactly once")
    void testAcquireIncrementsCounterOnce() {
        // Given: token in initial state, counter at 0
        assertEquals(0, counter.get());

        // When: acquire is called
        token.acquire();

        // Then: counter is incremented to 1
        assertEquals(1, counter.get());
    }

    @Test
    @DisplayName("multiple acquire() calls are idempotent")
    void testMultipleAcquireCallsAreIdempotent() {
        // Given: token in initial state
        assertEquals(0, counter.get());

        // When: acquire is called multiple times
        token.acquire();
        token.acquire();
        token.acquire();

        // Then: counter is incremented only once
        assertEquals(1, counter.get());
    }

    @Test
    @DisplayName("release() decrements counter exactly once after acquire()")
    void testReleaseDecrementsCounterOnce() {
        // Given: token has been acquired
        token.acquire();
        assertEquals(1, counter.get());

        // When: release is called
        token.release();

        // Then: counter is decremented to 0
        assertEquals(0, counter.get());
    }

    @Test
    @DisplayName("multiple release() calls are idempotent")
    void testMultipleReleaseCallsAreIdempotent() {
        // Given: token has been acquired and released once
        token.acquire();
        token.release();
        assertEquals(0, counter.get());

        // When: release is called multiple times
        token.release();
        token.release();
        token.release();

        // Then: counter remains at 0
        assertEquals(0, counter.get());
    }

    @Test
    @DisplayName("release() without acquire() has no effect")
    void testReleaseWithoutAcquireHasNoEffect() {
        // Given: token in initial state
        assertEquals(0, counter.get());

        // When: release is called without prior acquire
        token.release();

        // Then: counter remains at 0
        assertEquals(0, counter.get());
    }

    @Test
    @DisplayName("acquire() after release() has no effect")
    void testAcquireAfterReleaseHasNoEffect() {
        // Given: token has been acquired and released
        token.acquire();
        token.release();
        assertEquals(0, counter.get());

        // When: acquire is called again
        token.acquire();

        // Then: counter remains at 0 (token is in released state)
        assertEquals(0, counter.get());
    }

    @Test
    @DisplayName("complete lifecycle: initial -> acquired -> released")
    void testCompleteLifecycle() {
        // Initial state: counter should be 0
        assertEquals(0, counter.get());

        // Acquire: counter should be 1
        token.acquire();
        assertEquals(1, counter.get());

        // Release: counter should be 0
        token.release();
        assertEquals(0, counter.get());

        // Further operations should have no effect
        token.acquire();
        token.release();
        assertEquals(0, counter.get());
    }

    @Test
    @DisplayName("multiple tokens share the same counter correctly")
    void testMultipleTokensSharedCounter() {
        // Given: multiple tokens sharing the same counter
        ActivationToken token1 = new ActivationToken(counter);
        ActivationToken token2 = new ActivationToken(counter);
        ActivationToken token3 = new ActivationToken(counter);

        // When: each token is acquired
        token1.acquire();
        assertEquals(1, counter.get());

        token2.acquire();
        assertEquals(2, counter.get());

        token3.acquire();
        assertEquals(3, counter.get());

        // When: tokens are released
        token1.release();
        assertEquals(2, counter.get());

        token2.release();
        assertEquals(1, counter.get());

        token3.release();
        assertEquals(0, counter.get());
    }

    @Test
    @DisplayName("tokens are independent - one token's release doesn't affect others")
    void testTokensAreIndependent() {
        // Given: multiple tokens, some acquired, some not
        ActivationToken token1 = new ActivationToken(counter);
        ActivationToken token2 = new ActivationToken(counter);

        token1.acquire();
        // token2 remains unacquired
        assertEquals(1, counter.get());

        // When: releasing unacquired token
        token2.release();

        // Then: counter remains unchanged
        assertEquals(1, counter.get());

        // When: releasing acquired token
        token1.release();

        // Then: counter decrements properly
        assertEquals(0, counter.get());
    }

    @Test
    @DisplayName("concurrent acquire() operations are thread-safe")
    @Timeout(5)
    void testConcurrentAcquireOperations() throws InterruptedException {
        final int threadCount = 10;
        final ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        final CountDownLatch latch = new CountDownLatch(threadCount);
        final CyclicBarrier barrier = new CyclicBarrier(threadCount);

        try {
            // When: multiple threads try to acquire the same token concurrently
            IntStream.range(0, threadCount).forEach(i -> {
                executor.submit(() -> {
                    try {
                        barrier.await(); // Synchronize thread start
                        token.acquire();
                    }
                    catch (Exception e) {
                        fail("Unexpected exception: " + e.getMessage());
                    }
                    finally {
                        latch.countDown();
                    }
                });
            });

            // Wait for all threads to complete
            assertTrue(latch.await(3, TimeUnit.SECONDS));

            // Then: counter should be incremented exactly once
            assertEquals(1, counter.get());
        }
        finally {
            executor.shutdown();
            executor.awaitTermination(1, TimeUnit.SECONDS);
        }
    }

    @Test
    @DisplayName("concurrent acquire() and release() operations are thread-safe")
    @Timeout(5)
    void testConcurrentAcquireAndReleaseOperations() throws InterruptedException {
        final int threadCount = 20; // 10 acquire, 10 release
        final ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        final CountDownLatch latch = new CountDownLatch(threadCount);
        final CyclicBarrier barrier = new CyclicBarrier(threadCount);

        try {
            // When: multiple threads try to acquire and release concurrently
            IntStream.range(0, threadCount).forEach(i -> {
                executor.submit(() -> {
                    try {
                        barrier.await(); // Synchronize thread start
                        if (i < 10) {
                            token.acquire();
                        }
                        else {
                            token.release();
                        }
                    }
                    catch (Exception e) {
                        fail("Unexpected exception: " + e.getMessage());
                    }
                    finally {
                        latch.countDown();
                    }
                });
            });

            // Wait for all threads to complete
            assertTrue(latch.await(3, TimeUnit.SECONDS));

            // Then: final state should be consistent (either 0 or 1 depending on timing)
            int finalValue = counter.get();
            assertTrue(finalValue >= 0 && finalValue <= 1,
                    "Counter value should be 0 or 1, but was: " + finalValue);
        }
        finally {
            executor.shutdown();
            executor.awaitTermination(1, TimeUnit.SECONDS);
        }
    }

    @Test
    @DisplayName("concurrent operations on multiple tokens are thread-safe")
    @Timeout(5)
    void testConcurrentMultipleTokenOperations() throws InterruptedException {
        final int tokenCount = 5;
        final int threadsPerToken = 4;
        final int totalThreads = tokenCount * threadsPerToken;
        final ExecutorService executor = Executors.newFixedThreadPool(totalThreads);
        final CountDownLatch latch = new CountDownLatch(totalThreads);
        final ActivationToken[] tokens = new ActivationToken[tokenCount];

        // Create multiple tokens sharing the same counter
        for (int i = 0; i < tokenCount; i++) {
            tokens[i] = new ActivationToken(counter);
        }

        try {
            // When: multiple threads operate on different tokens concurrently
            for (int tokenIndex = 0; tokenIndex < tokenCount; tokenIndex++) {
                final int finalTokenIndex = tokenIndex;
                for (int threadIndex = 0; threadIndex < threadsPerToken; threadIndex++) {
                    final int finalThreadIndex = threadIndex;
                    executor.submit(() -> {
                        try {
                            ActivationToken currentToken = tokens[finalTokenIndex];
                            if (finalThreadIndex % 2 == 0) {
                                currentToken.acquire();
                            }
                            else {
                                currentToken.release();
                            }
                        }
                        catch (Exception e) {
                            fail("Unexpected exception: " + e.getMessage());
                        }
                        finally {
                            latch.countDown();
                        }
                    });
                }
            }

            // Wait for all threads to complete
            assertTrue(latch.await(5, TimeUnit.SECONDS));

            // Then: counter should be non-negative and not exceed token count
            int finalValue = counter.get();
            assertTrue(finalValue >= 0 && finalValue <= tokenCount,
                    "Counter value should be between 0 and " + tokenCount + ", but was: " + finalValue);
        }
        finally {
            executor.shutdown();
            executor.awaitTermination(1, TimeUnit.SECONDS);
        }
    }

    @RepeatedTest(10)
    @DisplayName("stress test - repeated concurrent operations")
    @Timeout(3)
    void stressTestRepeatedConcurrentOperations() throws InterruptedException {
        final int threadCount = 20;
        final int operationsPerThread = 10;
        final ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        final CountDownLatch latch = new CountDownLatch(threadCount);
        final AtomicReference<Exception> exceptionRef = new AtomicReference<>();

        try {
            IntStream.range(0, threadCount).forEach(i -> {
                executor.submit(() -> {
                    try {
                        for (int j = 0; j < operationsPerThread; j++) {
                            if (j % 2 == 0) {
                                token.acquire();
                            }
                            else {
                                token.release();
                            }
                        }
                    }
                    catch (Exception e) {
                        exceptionRef.set(e);
                    }
                    finally {
                        latch.countDown();
                    }
                });
            });

            assertTrue(latch.await(2, TimeUnit.SECONDS));
            assertNull(exceptionRef.get(), "No exceptions should occur during concurrent operations");

            // Counter should be in a valid state (0 or 1)
            int finalValue = counter.get();
            assertTrue(finalValue >= 0 && finalValue <= 1,
                    "Counter should be 0 or 1, but was: " + finalValue);
        }
        finally {
            executor.shutdown();
            executor.awaitTermination(1, TimeUnit.SECONDS);
        }
    }

    @Test
    @DisplayName("edge case - rapid acquire/release cycles")
    void testRapidAcquireReleaseCycles() {
        // Test rapid acquire/release cycles to ensure state consistency
        for (int i = 0; i < 1000; i++) {
            token.acquire();
            assertEquals(1, counter.get(), "Counter should be 1 after acquire in iteration " + i);

            token.release();
            assertEquals(0, counter.get(), "Counter should be 0 after release in iteration " + i);

            // Create new token for next iteration to reset state
            token = new ActivationToken(counter);
        }
    }

    @Test
    @DisplayName("counter starts from non-zero value")
    void testCounterStartsFromNonZeroValue() {
        // Given: counter starts at 42
        AtomicInteger nonZeroCounter = new AtomicInteger(42);
        ActivationToken nonZeroToken = new ActivationToken(nonZeroCounter);

        // When: token is acquired
        nonZeroToken.acquire();

        // Then: counter is incremented from its initial value
        assertEquals(43, nonZeroCounter.get());

        // When: token is released
        nonZeroToken.release();

        // Then: counter is decremented back to initial value
        assertEquals(42, nonZeroCounter.get());
    }

    @Test
    @DisplayName("negative counter values are handled correctly")
    void testNegativeCounterValues() {
        // Given: counter starts at -5
        AtomicInteger negativeCounter = new AtomicInteger(-5);
        ActivationToken negativeToken = new ActivationToken(negativeCounter);

        // When: token is acquired
        negativeToken.acquire();

        // Then: counter is incremented from negative value
        assertEquals(-4, negativeCounter.get());

        // When: token is released
        negativeToken.release();

        // Then: counter is decremented back to original negative value
        assertEquals(-5, negativeCounter.get());
    }
}
