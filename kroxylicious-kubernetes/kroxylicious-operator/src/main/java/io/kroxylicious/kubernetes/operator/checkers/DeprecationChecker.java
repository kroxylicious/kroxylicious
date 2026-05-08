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

    public static Cache<String, Boolean> getLogCache() {
        return LOG_CACHE;
    }

    public abstract void check(DeprecationCheckContext<S, T, R, F> context);

}
