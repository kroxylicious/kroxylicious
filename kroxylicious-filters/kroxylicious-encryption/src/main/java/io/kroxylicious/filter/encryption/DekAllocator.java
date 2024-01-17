/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;

import io.kroxylicious.kms.service.DekPair;
import io.kroxylicious.kms.service.Kms;

import edu.umd.cs.findbugs.annotations.NonNull;

// We want to allocate the same DEK to multiple channels
public class DekAllocator<K, E> {

    private final long maximumEncryptionsPerDek;
    private final AsyncLoadingCache<K, DekUsageContext<E>> cache;
    private static final int MAX_RETRIES = 3;

    record DekUsageContext<E>(DekPair<E> dek, AtomicLong remainingEncryptions, AtomicBoolean closed) {

        DekUsageContext(DekPair<E> dek, long maximumEncryptionsPerDek) {
            this(dek, new AtomicLong(maximumEncryptionsPerDek), new AtomicBoolean(false));
        }

        DekPair<E> allocate(long encryptions) {
            long remaining = remainingEncryptions.addAndGet(-encryptions);
            if (remaining >= 0) {
                return dek;
            }
            else {
                throw new ExhaustedException(this);
            }
        }

        // returns true if this is the first time it has been closed
        private boolean close() {
            boolean closedPrior = closed.getAndSet(true);
            return !closedPrior;
        }

    }

    public DekAllocator(@NonNull Kms<K, E> kms, long maximumEncryptionsPerDek) {
        this.maximumEncryptionsPerDek = maximumEncryptionsPerDek;
        cache = Caffeine.newBuilder().buildAsync(
                (key, executor) -> kms.generateDekPair(key).thenApply(dekPair -> new DekUsageContext<>(dekPair, this.maximumEncryptionsPerDek)).toCompletableFuture());
    }

    @NonNull
    public CompletableFuture<DekPair<E>> allocateDek(@NonNull K kekId, long encryptions) {
        if (encryptions > maximumEncryptionsPerDek) {
            return CompletableFuture
                    .failedFuture(new EncryptionException("DekAllocator asked to allocate encryptions above the configured maximum of " + maximumEncryptionsPerDek));
        }
        return allocateDek(kekId, encryptions, 0);
    }

    @NonNull
    private CompletableFuture<DekPair<E>> allocateDek(@NonNull K kekId, long encryptions, int attempt) {
        if (attempt == MAX_RETRIES) {
            return CompletableFuture.failedFuture(new EncryptionException("unable to allocate a DEK after " + MAX_RETRIES + " attempts"));
        }
        return cache.get(kekId).thenApply(eDekUsageContext -> eDekUsageContext.allocate(encryptions))
                .exceptionallyCompose(throwable -> invalidate(kekId, throwable, encryptions, attempt + 1));
    }

    @NonNull
    private CompletableFuture<DekPair<E>> invalidate(@NonNull K kekId, Throwable throwable, long encryptions, int nextAttempt) {
        if (throwable instanceof ExhaustedException ex) {
            invalidate(kekId, ex);
        }
        else if (throwable instanceof CompletionException e && e.getCause() instanceof ExhaustedException cause) {
            invalidate(kekId, cause);
        }
        return allocateDek(kekId, encryptions, nextAttempt);
    }

    private void invalidate(@NonNull K kekId, ExhaustedException ex) {
        boolean firstClose = ex.context.close();
        if (firstClose) {
            cache.synchronous().invalidate(kekId);
        }
    }

    private static class ExhaustedException extends RuntimeException {
        private final DekUsageContext<?> context;

        private ExhaustedException(DekUsageContext<?> context) {
            this.context = context;
        }
    }
}
