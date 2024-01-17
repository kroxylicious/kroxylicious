/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import javax.crypto.SecretKey;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.kroxylicious.kms.service.DekPair;
import io.kroxylicious.kms.service.Kms;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DekAllocatorTest {

    public static final int MAXIMUM_ENCRYPTIONS_PER_DEK = 1000;
    @Mock
    Kms<Long, Long> kms;
    @Mock
    SecretKey secretKey;
    @Mock
    SecretKey secretKey2;

    @Test
    public void testInitialAllocationLessThanMaximumPerDek() {
        // given
        DekPair<Long> expected = new DekPair<>(1L, secretKey);
        when(kms.generateDekPair(1L)).thenReturn(CompletableFuture.completedFuture(expected));
        DekAllocator<Long, Long> allocator = new DekAllocator<>(kms, MAXIMUM_ENCRYPTIONS_PER_DEK);

        // when
        CompletableFuture<DekPair<Long>> pair = allocator.allocateDek(1L, 500);

        // then
        assertThat(pair).succeedsWithin(Duration.ZERO).isSameAs(expected);
        verify(kms).generateDekPair(1L);
    }

    @Test
    public void testSameDekCanBeAllocatedMultipleTimes() {
        // given
        DekPair<Long> expected = new DekPair<>(1L, secretKey);
        when(kms.generateDekPair(1L)).thenReturn(CompletableFuture.completedFuture(expected));
        DekAllocator<Long, Long> allocator = new DekAllocator<>(kms, MAXIMUM_ENCRYPTIONS_PER_DEK);

        // when
        CompletableFuture<DekPair<Long>> pair1 = allocator.allocateDek(1L, 500);
        CompletableFuture<DekPair<Long>> pair2 = allocator.allocateDek(1L, 500);

        // then
        assertThat(pair1).succeedsWithin(Duration.ZERO).isSameAs(expected);
        assertThat(pair2).succeedsWithin(Duration.ZERO).isSameAs(expected);
        verify(kms).generateDekPair(1L);
    }

    @Test
    public void testAllocatorGeneratesANewDekIfMaxEncryptionsWouldBeExceeded() {
        // given
        DekPair<Long> expected = new DekPair<>(1L, secretKey);
        DekPair<Long> expected2 = new DekPair<>(4L, secretKey2);
        when(kms.generateDekPair(1L)).thenReturn(CompletableFuture.completedFuture(expected), CompletableFuture.completedFuture(expected2));
        DekAllocator<Long, Long> allocator = new DekAllocator<>(kms, MAXIMUM_ENCRYPTIONS_PER_DEK);

        // when
        CompletableFuture<DekPair<Long>> pair1 = allocator.allocateDek(1L, 500);
        CompletableFuture<DekPair<Long>> pair2 = allocator.allocateDek(1L, 600);

        // then
        assertThat(pair1).succeedsWithin(Duration.ZERO).isSameAs(expected);
        assertThat(pair2).succeedsWithin(Duration.ZERO).isSameAs(expected2);
        verify(kms, times(2)).generateDekPair(1L);
    }

    @Test
    public void testInitialAllocationEqualToMaximumPerDek() {
        // given
        DekPair<Long> expected = new DekPair<>(1L, secretKey);
        when(kms.generateDekPair(1L)).thenReturn(CompletableFuture.completedFuture(expected));
        DekAllocator<Long, Long> allocator = new DekAllocator<>(kms, MAXIMUM_ENCRYPTIONS_PER_DEK);

        // when
        CompletableFuture<DekPair<Long>> pair = allocator.allocateDek(1L, 1000);

        // then
        assertThat(pair).succeedsWithin(Duration.ZERO).isSameAs(expected);
        verify(kms).generateDekPair(1L);
    }

    @Test
    public void testInitialAllocationGreaterThanMaximumPerDek() {
        // given
        DekAllocator<Long, Long> allocator = new DekAllocator<>(kms, MAXIMUM_ENCRYPTIONS_PER_DEK);

        // when
        CompletableFuture<DekPair<Long>> pair = allocator.allocateDek(1L, 1001);

        // then
        assertThat(pair).failsWithin(Duration.ZERO).withThrowableThat().isInstanceOf(ExecutionException.class)
                .havingCause().isInstanceOf(EncryptionException.class);
        verify(kms, never()).generateDekPair(anyLong());
    }

    @Test
    public void testDefaultMaxAllocation() {
        // given
        DekPair<Long> expected = new DekPair<>(1L, secretKey);
        when(kms.generateDekPair(1L)).thenReturn(CompletableFuture.completedFuture(expected));
        DekAllocator<Long, Long> allocator = new DekAllocator<>(kms, (long) Math.pow(2, 32));

        // when
        CompletableFuture<DekPair<Long>> pair = allocator.allocateDek(1L, (long) Math.pow(2, 32));

        // then
        assertThat(pair).succeedsWithin(Duration.ZERO).isSameAs(expected);
        verify(kms).generateDekPair(1L);
    }

    @Test
    public void testDefaultMaxAllocationExceeded() {
        // given
        DekAllocator<Long, Long> allocator = new DekAllocator<>(kms, (long) Math.pow(2, 32));

        // when
        CompletableFuture<DekPair<Long>> pair = allocator.allocateDek(1L, (long) Math.pow(2, 32) + 1L);

        // then
        assertThat(pair).failsWithin(Duration.ZERO).withThrowableThat().isInstanceOf(ExecutionException.class)
                .havingCause().isInstanceOf(EncryptionException.class);
        verify(kms, never()).generateDekPair(anyLong());
    }

}
