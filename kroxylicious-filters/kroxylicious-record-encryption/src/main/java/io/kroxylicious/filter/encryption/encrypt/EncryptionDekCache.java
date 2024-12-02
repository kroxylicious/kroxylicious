/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.encrypt;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;

import io.kroxylicious.filter.encryption.common.FilterThreadExecutor;
import io.kroxylicious.filter.encryption.config.CipherSpec;
import io.kroxylicious.filter.encryption.dek.CipherSpecResolver;
import io.kroxylicious.filter.encryption.dek.Dek;
import io.kroxylicious.filter.encryption.dek.DekManager;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * A cache of DEKs used on the encryption path.
 * @param <K> The type of KEK id.
 * @param <E> The type of encrypted DEK.
 */
public class EncryptionDekCache<K, E> {

    private static final Logger LOGGER = LoggerFactory.getLogger(EncryptionDekCache.class);

    public static final int NO_MAX_CACHE_SIZE = -1;
    private CipherSpecResolver cipherSpecResolver;
    private AtomicLong invalidationCount = new AtomicLong(0);

    private record CacheKey<K>(K kek, CipherSpec cipherSpec) {}

    public class EncryptionDek {
        private final CacheKey<K> key;
        private final Dek<E> dek;
        private final AtomicBoolean hasBeenRotated = new AtomicBoolean(false);
        private final CompletableFuture<Void> rotated = new CompletableFuture<>();

        public EncryptionDek(CacheKey<K> key, Dek<E> dek) {
            this.key = key;
            this.dek = dek;
        }

        /**
         * Destroy this DEK for encryption
         * Discard the currently cached DEK for the KEK in the given {@code encryptionScheme} once.
         * Competing threads will only discard it from the cache once.
         * This method may block if a DEK for the given {@code encryptionScheme} is in the process
         * of being loaded.
         */
        public void rotate() {
            if (hasBeenRotated.compareAndSet(false, true)) {
                LOGGER.debug("rotating encryption key {}", key);
                dek.destroyForEncrypt();
                EncryptionDekCache.this.invalidate(key);
                rotated.complete(null);
            }
            else {
                LOGGER.debug("encryption key {} has already been rotated", key);
            }
        }

        /**
         * @return a future that is completed after cache invalidation, enabling multiple threads
         * to wait for cache invalidation before retrying the encryption operation.
         */
        public CompletionStage<Void> rotateFuture() {
            return rotated;
        }

        public Dek<E> getDek() {
            return dek;
        }
    }

    private static <K> CacheKey<K> cacheKey(EncryptionScheme<K> encryptionScheme) {
        return new CacheKey<>(encryptionScheme.kekId(), CipherSpec.AES_256_GCM_128);
    }

    private final DekManager<K, E> dekManager;

    private final AsyncLoadingCache<CacheKey<K>, EncryptionDek> dekCache;

    public EncryptionDekCache(@NonNull DekManager<K, E> dekManager,
                              @Nullable Executor dekCacheExecutor,
                              int dekCacheMaxItems,
                              @NonNull Duration refreshAfterWrite,
                              @NonNull Duration expireAfterWrite) {
        Objects.requireNonNull(refreshAfterWrite, "refreshAfterWrite is null");
        Objects.requireNonNull(expireAfterWrite, "expireAfterWrite is null");
        this.dekManager = Objects.requireNonNull(dekManager);
        this.cipherSpecResolver = CipherSpecResolver.ALL;
        Caffeine<Object, Object> cache = Caffeine.newBuilder();
        if (dekCacheMaxItems != NO_MAX_CACHE_SIZE) {
            cache = cache.maximumSize(dekCacheMaxItems);
        }
        if (dekCacheExecutor != null) {
            cache = cache.executor(dekCacheExecutor);
        }
        cache = cache.refreshAfterWrite(refreshAfterWrite);
        cache = cache.expireAfterWrite(expireAfterWrite);

        this.dekCache = cache
                .removalListener(this::afterCacheEviction)
                .buildAsync(this::requestGenerateDek);
    }

    /**
     * Invoked by Caffeine when a DEK needs to be loaded.
     * This method is executed on the {@code dekCacheExecutor} passed to the constructor.
     */
    private CompletableFuture<EncryptionDek> requestGenerateDek(@NonNull CacheKey<K> cacheKey,
                                                                @NonNull Executor executor) {
        return dekManager.generateDek(cacheKey.kek(), cipherSpecResolver.fromName(cacheKey.cipherSpec()))
                .thenApply(dek -> {
                    if (LOGGER.isTraceEnabled()) {
                        LOGGER.trace("Adding DEK to cache: {}", dek);
                    }
                    dek.destroyForDecrypt();
                    return dek;
                })
                .thenApply(eDek -> new EncryptionDek(cacheKey, eDek))
                .toCompletableFuture();
    }

    /**
     * Invoked by Caffeine after a DEK is evicted from the cache.
     * This method is executed on the {@code dekCacheExecutor} passed to the constructor.
     */
    private void afterCacheEviction(@Nullable CacheKey<K> cacheKey,
                                    @Nullable EncryptionDek dek,
                                    RemovalCause removalCause) {
        if (dek != null) {
            dek.getDek().destroyForEncrypt();
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("Attempted to destroy DEK: {}", dek);
            }
        }
    }

    /**
     * Obtain a Dek for the KEK in the given {@code encryptionScheme},
     * generating a new one if necessary.
     *
     * @param encryptionScheme The KEK to get a DEK for.
     * @param filterThreadExecutor The filter thread executor.
     * @return A stage that completes on the filter thread with the DEK.
     */
    public @NonNull CompletionStage<EncryptionDek> get(@NonNull EncryptionScheme<K> encryptionScheme,
                                                       @NonNull FilterThreadExecutor filterThreadExecutor) {
        return filterThreadExecutor.completingOnFilterThread(dekCache.get(cacheKey(encryptionScheme)));
    }

    /**
     * Discard any cached DEK for the KEK in the given {@code encryptionScheme}.
     * This method may block if a DEK for the given {@code encryptionScheme} is in the process
     * of being loaded.
     * @param encryptionScheme The KEK for the DEK to discard.
     */
    public void invalidate(@NonNull EncryptionScheme<K> encryptionScheme) {
        CacheKey<K> key = cacheKey(encryptionScheme);
        invalidate(key);
    }

    private void invalidate(@NonNull CacheKey<K> key) {
        invalidationCount.incrementAndGet();
        dekCache.synchronous().invalidate(key);
    }

    public long invalidationCount() {
        return invalidationCount.get();
    }
}
