/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.kms;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;

import javax.crypto.SecretKey;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import io.kroxylicious.kms.service.DekPair;
import io.kroxylicious.kms.service.Kms;
import io.kroxylicious.kms.service.Serde;
import io.kroxylicious.kms.service.UnknownAliasException;
import io.kroxylicious.proxy.tag.VisibleForTesting;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Kms implementation that caches results from a delegate where appropriate.
 * Resolved aliases can be cached temporarily as the key they point to could be
 * changed (by rotation for example), a theoretically infrequent operation.
 * Decrypted DEKs can be cached more aggressively since it is a deterministic
 * mapping from eDEK to DEK on the decrypt path.
 * @param <K> The type of Key Encryption Key id.
 * @param <E> The type of encrypted Data Encryption Key.
 */
public class CachingKms<K, E> implements Kms<K, E> {
    private final Kms<K, E> delegate;
    private final AsyncLoadingCache<E, SecretKey> decryptDekCache;
    private final AsyncLoadingCache<String, K> resolveAliasCache;
    private final Cache<String, CompletionStage<K>> notFoundAliasCache;
    private static final Logger LOGGER = LoggerFactory.getLogger(CachingKms.class);

    private CachingKms(@NonNull Kms<K, E> delegate,
                       long decryptDekCacheMaxSize,
                       @NonNull Duration decryptDekExpireAfterAccess,
                       long resolveAliasCacheMaxSize,
                       @NonNull Duration resolveAliasExpireAfterWrite,
                       @NonNull Duration resolveAliasRefreshAfterWrite,
                       @NonNull Duration notFoundAliasExpireAfterWrite) {
        this.delegate = delegate;
        decryptDekCache = buildDecryptedDekCache(delegate, decryptDekCacheMaxSize, decryptDekExpireAfterAccess);
        resolveAliasCache = buildResolveAliasCache(delegate, resolveAliasCacheMaxSize, resolveAliasExpireAfterWrite, resolveAliasRefreshAfterWrite);
        notFoundAliasCache = Caffeine.newBuilder().expireAfterWrite(notFoundAliasExpireAfterWrite).build();
    }

    /**
     * Wraps the given KMS with caching of resolved aliases and decrypted DEKs.
     * @param delegate the KMS to be wrapped.
     * @param decryptDekCacheMaxSize the maximum number of decrypted DEKs to cache.
     * @param decryptDekExpireAfterAccess how long after last access a decrypted DEK is removed from the cache.
     * @param resolveAliasCacheMaxSize the maximum number of resolved aliases to cache.
     * @param resolveAliasExpireAfterWrite how long after being cached a resolved alias is removed from the cache.
     * @param resolveAliasRefreshAfterWrite how long after being cached a resolved alias becomes eligible for a refresh.
     * @param notFoundAliasExpireAfterWrite how long an unresolvable alias is remembered before the delegate is asked to resolve it again.
     * @param <A> The type of Key Encryption Key id.
     * @param <B> The type of encrypted Data Encryption Key.
     * @return a caching KMS wrapping the delegate.
     */
    @NonNull
    public static <A, B> Kms<A, B> wrap(Kms<A, B> delegate,
                                        long decryptDekCacheMaxSize,
                                        Duration decryptDekExpireAfterAccess,
                                        long resolveAliasCacheMaxSize,
                                        Duration resolveAliasExpireAfterWrite,
                                        Duration resolveAliasRefreshAfterWrite,
                                        Duration notFoundAliasExpireAfterWrite) {
        return new CachingKms<>(delegate, decryptDekCacheMaxSize, decryptDekExpireAfterAccess, resolveAliasCacheMaxSize, resolveAliasExpireAfterWrite,
                resolveAliasRefreshAfterWrite, notFoundAliasExpireAfterWrite);
    }

    @NonNull
    private static <K, E> AsyncLoadingCache<String, K> buildResolveAliasCache(Kms<K, E> delegate, long maxSize, Duration expireAfterWrite, Duration refreshAfterWrite) {
        return Caffeine.newBuilder().maximumSize(maxSize).refreshAfterWrite(refreshAfterWrite).expireAfterWrite(expireAfterWrite)
                .buildAsync((key, executor) -> delegate.resolveAlias(key).toCompletableFuture());
    }

    @NonNull
    private static <K, E> AsyncLoadingCache<E, SecretKey> buildDecryptedDekCache(Kms<K, E> delegate, long maxSize, Duration expireAfterAccess) {
        return Caffeine.newBuilder().maximumSize(maxSize).expireAfterAccess(expireAfterAccess)
                .buildAsync((key, executor) -> delegate.decryptEdek(key).toCompletableFuture());
    }

    @NonNull
    @Override
    public CompletionStage<DekPair<E>> generateDekPair(@NonNull K kekRef) {
        return delegate.generateDekPair(kekRef);
    }

    @NonNull
    @Override
    public CompletionStage<SecretKey> decryptEdek(@NonNull E edek) {
        return decryptDekCache.get(edek);
    }

    @NonNull
    @Override
    public Serde<E> edekSerde() {
        return delegate.edekSerde();
    }

    // FutureReturnValueIgnored: the failure is handled inside the callback; the throwable is
    // inspected to populate the not-found cache, and `resolved` is returned to the caller
    // on the next line.
    @SuppressWarnings("FutureReturnValueIgnored")
    @NonNull
    @Override
    public CompletionStage<K> resolveAlias(@NonNull String alias) {
        CompletionStage<K> cachedNotFound = notFoundAliasCache.getIfPresent(alias);
        if (cachedNotFound != null) {
            return cachedNotFound;
        }
        CompletableFuture<K> resolved = resolveAliasCache.get(alias);
        resolved.whenComplete((k, throwable) -> {
            if (throwable instanceof UnknownAliasException || (throwable instanceof CompletionException && throwable.getCause() instanceof UnknownAliasException)) {
                LOGGER.atDebug()
                        .addKeyValue("kekId", alias)
                        .log("Caching unknown alias");
                notFoundAliasCache.put(alias, CompletableFuture.failedFuture(new UnknownAliasException("alias " + alias + " not found (cached result)")));
            }
        });
        return resolved;
    }

    @VisibleForTesting
    void decryptDekCacheCleanUp() {
        decryptDekCache.synchronous().cleanUp();
    }

    @VisibleForTesting
    void resolveAliasCacheCleanUp() {
        resolveAliasCache.synchronous().cleanUp();
    }

    @VisibleForTesting
    void notFoundAliasCacheCleanUp() {
        notFoundAliasCache.cleanUp();
    }

}
