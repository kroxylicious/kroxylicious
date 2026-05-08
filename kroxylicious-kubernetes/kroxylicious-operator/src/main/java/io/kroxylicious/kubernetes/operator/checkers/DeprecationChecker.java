/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.checkers;

import java.time.Duration;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import io.fabric8.kubernetes.client.CustomResource;

import io.kroxylicious.kubernetes.operator.StatusFactory;

/**
 * Checks a custom resource for use of deprecated features and, when found, appends the
 * appropriate {@link io.kroxylicious.kubernetes.api.common.Condition} entries to the
 * context's condition list ({@link DeprecationCheckContext#conditions()}) and optionally logs something.
 * <p>
 * All subclasses share a single cache (see {@link #getLogCache()}) used to suppress repeated
 * log warnings for the same resource within a certain time window. Because the cache is
 * shared, each subclass <em>must</em> use a unique string prefix in its cache keys to avoid
 * collisions with other checkers. The recommended convention is {@code "<checker-name>/<uid>"},
 * e.g. {@code "absent-spec/<uid>"}.
 *
 * @param <S> the spec type of the custom resource
 * @param <T> the status type of the custom resource
 * @param <R> the custom resource type
 * @param <F> the status factory type used to construct conditions
 *
 * @see DeprecationCheckContext
 */
public abstract class DeprecationChecker<S, T, R extends CustomResource<S, T>, F extends StatusFactory<R>> {
    private static final Cache<String, Boolean> LOG_CACHE = Caffeine.newBuilder()
            .expireAfterWrite(Duration.ofHours(1))
            .maximumSize(100)
            .build();

    /**
     * Returns the shared log-suppression cache used by all checker subclasses.
     * Subclasses must prefix their cache keys with a unique string to avoid key collisions.
     *
     * @return the shared cache
     */
    protected static Cache<String, Boolean> getLogCache() {
        return LOG_CACHE;
    }

    /**
     * Performs the deprecation check against the resource in {@code context}.
     * Implementations should append any applicable {@link io.kroxylicious.kubernetes.api.common.Condition}
     * entries to {@link DeprecationCheckContext#conditions()} and emit log warnings via
     * {@link DeprecationCheckContext#logger()}, using the shared log cache to suppress repeated warnings.
     *
     * @param context carries the resource, logger, status factory, and mutable condition list for this check pass
     */
    public abstract void check(DeprecationCheckContext<S, T, R, F> context);

}
