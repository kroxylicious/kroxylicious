/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import javax.crypto.SecretKey;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import io.kroxylicious.kms.service.DekPair;
import io.kroxylicious.kms.service.Kms;
import io.kroxylicious.kms.service.Serde;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class CachingKmsTest {

    @Test
    void testEdekSerdeDelegated() {
        Kms<Long, Long> kms = mock(Kms.class);
        Serde mockSerde = mock(Serde.class);
        Mockito.when(kms.edekSerde()).thenReturn(mockSerde);
        Kms<Long, Long> caching = CachingKms.wrap(kms, 1L, Duration.ZERO, 1L, Duration.ZERO, Duration.ofMinutes(8));
        Serde<Long> longSerde = caching.edekSerde();
        assertThat(longSerde).isSameAs(mockSerde);
        verify(kms).edekSerde();
    }

    @Test
    void testGenerateDekPairDelegated() {
        Kms<Long, Long> kms = mock(Kms.class);
        DekPair<Long> eDekPair = new DekPair<>(1L, mock(SecretKey.class));
        Mockito.when(kms.generateDekPair(any())).thenReturn(CompletableFuture.completedFuture(eDekPair));
        Kms<Long, Long> caching = CachingKms.wrap(kms, 1L, Duration.ZERO, 1L, Duration.ZERO, Duration.ofMinutes(8));
        CompletionStage<DekPair<Long>> dekPairCompletionStage = caching.generateDekPair(1L);
        assertThat(dekPairCompletionStage).succeedsWithin(5, TimeUnit.SECONDS).isSameAs(eDekPair);
        verify(kms).generateDekPair(1L);
    }

    @Test
    void testDecryptEdekCached() {
        Kms<Long, Long> kms = mock(Kms.class);
        SecretKey secretKey = mock(SecretKey.class);
        Mockito.when(kms.decryptEdek(any())).thenReturn(CompletableFuture.completedFuture(secretKey));
        Kms<Long, Long> caching = CachingKms.wrap(kms, 1L, Duration.ofHours(1), 1L, Duration.ZERO, Duration.ofMinutes(8));
        assertThat(caching.decryptEdek(1L)).succeedsWithin(5, TimeUnit.SECONDS).isSameAs(secretKey);
        assertThat(caching.decryptEdek(1L)).succeedsWithin(5, TimeUnit.SECONDS).isSameAs(secretKey);
        verify(kms, times(1)).decryptEdek(1L);
    }

    @Test
    void testDecryptEdekNotCachedIfExpiryZero() {
        Kms<Long, Long> kms = mock(Kms.class);
        SecretKey secretKey = mock(SecretKey.class);
        Mockito.when(kms.decryptEdek(any())).thenReturn(CompletableFuture.completedFuture(secretKey));
        Kms<Long, Long> caching = CachingKms.wrap(kms, 1L, Duration.ZERO, 1L, Duration.ZERO, Duration.ofMinutes(8));
        assertThat(caching.decryptEdek(1L)).succeedsWithin(5, TimeUnit.SECONDS).isSameAs(secretKey);
        assertThat(caching.decryptEdek(1L)).succeedsWithin(5, TimeUnit.SECONDS).isSameAs(secretKey);
        verify(kms, times(2)).decryptEdek(1L);
    }

    @Test
    void testResolveAliasCached() {
        Kms<Long, Long> kms = mock(Kms.class);
        long kekId = 2L;
        Mockito.when(kms.resolveAlias(any())).thenReturn(CompletableFuture.completedFuture(kekId));
        Kms<Long, Long> caching = CachingKms.wrap(kms, 1L, Duration.ZERO, 1L, Duration.ofHours(1), Duration.ofMinutes(8));
        assertThat(caching.resolveAlias("a")).succeedsWithin(5, TimeUnit.SECONDS).isEqualTo(kekId);
        assertThat(caching.resolveAlias("a")).succeedsWithin(5, TimeUnit.SECONDS).isEqualTo(kekId);
        verify(kms, times(1)).resolveAlias("a");
    }

    @Test
    void testResolveAliasNotCachedIfExpiryZero() {
        Kms<Long, Long> kms = mock(Kms.class);
        long kekId = 2L;
        Mockito.when(kms.resolveAlias(any())).thenReturn(CompletableFuture.completedFuture(kekId));
        Kms<Long, Long> caching = CachingKms.wrap(kms, 1L, Duration.ZERO, 1L, Duration.ZERO, Duration.ofMinutes(8));
        assertThat(caching.resolveAlias("a")).succeedsWithin(5, TimeUnit.SECONDS).isEqualTo(kekId);
        assertThat(caching.resolveAlias("a")).succeedsWithin(5, TimeUnit.SECONDS).isEqualTo(kekId);
        verify(kms, times(2)).resolveAlias("a");
    }

}
