/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.decrypt;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;

import io.kroxylicious.filter.encryption.common.FilterThreadExecutor;
import io.kroxylicious.filter.encryption.dek.CipherManager;
import io.kroxylicious.filter.encryption.dek.Dek;
import io.kroxylicious.filter.encryption.dek.DekManager;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * A cache of DEKs used on the decryption path.
 * @param <K> The type of KEK id.
 * @param <E> The type of encrypted DEK.
 */
public class DecryptionDekCache<K, E> {

    private static final Logger LOGGER = LoggerFactory.getLogger(DecryptionDekCache.class);

    public static final int NO_MAX_CACHE_SIZE = -1;
    private final DekManager<K, E> dekManager;

    public record CacheKey<E>(
            @Nullable
            CipherManager cipherManager,
            @Nullable
            E edek
    ) {
        public CacheKey {
            if (cipherManager == null ^ edek == null) {
                throw new IllegalArgumentException();
            }
        }

        /** A sentinel for records which are not encrypted */
        @SuppressWarnings({ "rawtypes", "unchecked" })
        private static final CacheKey UNENCRYPTED = new CacheKey(null, null);

        /** Gets the sentinel value for records which are not encrypted */
        @SuppressWarnings("unchecked")
        static <E> CacheKey<E> unencrypted() {
            return UNENCRYPTED;
        }

        /** Tests whether this cache key is the sentinel value representing unencrypted records */
        public boolean isUnencrypted() {
            return cipherManager == null || edek == null;
        }
    }

    private final AsyncLoadingCache<CacheKey<E>, Dek<E>> decryptorCache;

    public DecryptionDekCache(
            @NonNull
            DekManager<K, E> dekManager,
            @Nullable
            Executor dekCacheExecutor,
            int dekCacheMaxItems
    ) {
        this.dekManager = Objects.requireNonNull(dekManager);
        Caffeine<Object, Object> cache = Caffeine.<CacheKey<E>, Dek<E>> newBuilder();
        if (dekCacheMaxItems != NO_MAX_CACHE_SIZE) {
            cache.maximumSize(dekCacheMaxItems);
        }
        if (dekCacheExecutor != null) {
            cache.executor(dekCacheExecutor);
        }
        this.decryptorCache = cache
                                   .removalListener(this::afterCacheEviction)
                                   .buildAsync(this::loadDek);
    }

    /**
     * Invoked by Caffeine when a DEK needs to be loaded.
     * This method is executed on the {@code dekCacheExecutor} passed to the constructor.
     * @param cacheKey The cache key
     * @param executor The executor
     * @return A future
     */
    private CompletableFuture<Dek<E>> loadDek(CacheKey<E> cacheKey, Executor executor) {
        if (cacheKey == null || cacheKey.isUnencrypted()) {
            return CompletableFuture.completedFuture(null);
        }
        // This assert is just to appease sonar because it doesn't grok that these things cannot be null due to
        // the cacheKey.isNone() check above
        assert cacheKey.edek() != null && cacheKey.cipherManager() != null;
        return dekManager.decryptEdek(cacheKey.edek(), cacheKey.cipherManager())
                         .toCompletableFuture();
    }

    /**
     * Invoked by Caffeine after a DEK is evicted from the cache.
     * This method is executed on the {@code dekCacheExecutor} passed to the constructor.
     */
    private void afterCacheEviction(
            @Nullable
            CacheKey<E> cacheKey,
            @Nullable
            Dek<E> dek,
            RemovalCause removalCause
    ) {
        if (dek != null) {
            dek.destroyForDecrypt();
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("Attempted to destroy DEK: {}", dek);
            }
        }
    }

    /**
     * Gets the DEKs for all the given cache keys
     * @param cacheKeys The cache key
     * @param filterThreadExecutor The filter thread executor
     * @return A completion stage which completes on the filter thread with the DEKs for the given {@code cacheKeys}.
     */
    public @NonNull CompletionStage<Map<CacheKey<E>, Dek<E>>> getAll(
            @NonNull
            List<CacheKey<E>> cacheKeys,
            @NonNull
            FilterThreadExecutor filterThreadExecutor
    ) {
        return filterThreadExecutor.completingOnFilterThread(decryptorCache.getAll(cacheKeys));
    }
}
