/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.inband;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;

import io.kroxylicious.filter.encryption.EncryptionScheme;
import io.kroxylicious.filter.encryption.dek.CipherSpec;
import io.kroxylicious.filter.encryption.dek.Dek;
import io.kroxylicious.filter.encryption.dek.DekManager;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

public class EncryptionDekCache<K, E> {

    private static final Logger LOGGER = LoggerFactory.getLogger(EncryptionDekCache.class);

    public static final int NO_MAX_CACHE_SIZE = -1;

    private record CacheKey<K>(K kek, CipherSpec cipherSpec) {}

    private static <K> CacheKey<K> cacheKey(EncryptionScheme<K> encryptionScheme) {
        return new CacheKey<>(encryptionScheme.kekId(), CipherSpec.AES_128_GCM_128);
    }

    private final DekManager<K, E> dekManager;

    private final AsyncLoadingCache<CacheKey<K>, Dek<E>> dekCache;

    public EncryptionDekCache(
                              @NonNull DekManager<K, E> dekManager,
                              @Nullable Executor dekCacheExecutor,
                              int dekCacheMaxItems) {
        this.dekManager = dekManager;
        Caffeine<Object, Object> cache = Caffeine.newBuilder();
        if (dekCacheMaxItems != NO_MAX_CACHE_SIZE) {
            cache = cache.maximumSize(dekCacheMaxItems);
        }
        if (dekCacheExecutor != null) {
            cache = cache.executor(dekCacheExecutor);
        }

        this.dekCache = cache
                .removalListener(this::afterCacheEviction)
                .buildAsync(this::requestGenerateDek);
    }

    /** Invoked by Caffeine when a DEK needs to be loaded */
    private CompletableFuture<Dek<E>> requestGenerateDek(@NonNull CacheKey<K> cacheKey,
                                                         @NonNull Executor executor) {
        return dekManager.generateDek(cacheKey.kek(), cacheKey.cipherSpec())
                .thenApply(dek -> {
                    if (LOGGER.isTraceEnabled()) {
                        LOGGER.trace("Adding DEK to cache: {}", dek);
                    }
                    dek.destroyForDecrypt();
                    return dek;
                })
                .toCompletableFuture();
    }

    /** Invoked by Caffeine after a DEK is evicted from the cache. */
    private void afterCacheEviction(@Nullable CacheKey<K> cacheKey,
                                    @Nullable Dek<E> dek,
                                    RemovalCause removalCause) {
        if (dek != null) {
            dek.destroyForEncrypt();
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("Attempted to destroy DEK: {}", dek);
            }
        }
    }

    public CompletionStage<Dek<E>> get(EncryptionScheme<K> encryptionScheme) {
        return dekCache.get(cacheKey(encryptionScheme));
    }

    public void invalidate(EncryptionScheme<K> encryptionScheme) {
        dekCache.synchronous().invalidate(cacheKey(encryptionScheme));
    }
}
