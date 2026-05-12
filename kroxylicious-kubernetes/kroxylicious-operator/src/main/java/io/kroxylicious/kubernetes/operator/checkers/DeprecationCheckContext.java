/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.checkers;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import io.fabric8.kubernetes.client.CustomResource;

import io.kroxylicious.kubernetes.api.common.Condition;
import io.kroxylicious.kubernetes.operator.OperatorLoggingKeys;
import io.kroxylicious.kubernetes.operator.ResourcesUtil;
import io.kroxylicious.kubernetes.operator.StatusFactory;

/**
 * Carries the inputs and outputs for a single deprecation check pass.
 * <p>
 * Checkers call {@link #addConditionAndLogWarning} to simultaneously append a {@link Condition} and
 * log a warning.
 * <p>
 * When a deprecation is corrected, checkers should call {@link #invalidateLogCacheEntry}
 * so that the warning will be re-emitted if the deprecation is re-introduced later.
 * <p>
 * After all checkers have run, the accumulated conditions can be retrieved via {@link #conditions()}.
 *
 * @param <S> the spec type of the custom resource
 * @param <T> the status type of the custom resource
 * @param <R> the custom resource type
 * @param <F> the status factory type
 *
 * @see DeprecationChecker
 */
public class DeprecationCheckContext<S, T, R extends CustomResource<S, T>, F extends StatusFactory<R>> {

    private static final Cache<String, Boolean> LOG_CACHE = Caffeine.newBuilder()
            .expireAfterWrite(Duration.ofHours(1))
            .maximumSize(100)
            .build();

    private final R resource;
    private final Logger logger;
    private final F statusFactory;
    private final List<Condition> conditions = new ArrayList<>();

    public DeprecationCheckContext(R resource, Logger logger, F statusFactory) {
        this.resource = resource;
        this.logger = logger;
        this.statusFactory = statusFactory;
    }

    /**
     * Returns the custom resource being checked.
     *
     * @return the custom resource
     */
    public R resource() {
        return resource;
    }

    /**
     * Appends a {@link Condition} to {@link #conditions()} without logging.
     * <p>
     * Preferably use {@link #addConditionAndLogWarning} instead.
     *
     * @param type the condition type (typically {@link Condition.Type#DeprecationWarning})
     * @param message the human-readable message attached to the condition
     *
     * @see #addConditionAndLogWarning
     */
    public void addCondition(Condition.Type type, String message) {
        conditions.add(statusFactory.newTrueCondition(resource, type, message));
    }

    /**
     * Appends a {@link Condition} to {@link #conditions()} and, if the {@code logMessageCacheKey}
     * is not present in the log cache, logs a warning.
     *
     * <p>Subclasses of {@link DeprecationChecker} <em>must</em> use a unique string prefix
     * in each {@code logMessageCacheKey} to avoid collisions with other checkers. The
     * recommended convention is {@code "<checker-name>/<uid>"}, e.g.
     * {@code "absent-spec/<uid>"}.
     *
     * @param type the condition type (typically {@link Condition.Type#DeprecationWarning})
     * @param message the human-readable message that is logged and attached to the condition
     * @param logMessageCacheKey unique key used to suppress repeated log emissions
     */
    @SuppressWarnings("java:S2583") // false positive, .putIfAbsent() can return null
    public void addConditionAndLogWarning(Condition.Type type, String message, String logMessageCacheKey) {
        if (LOG_CACHE.asMap().putIfAbsent(logMessageCacheKey, true) == null) {
            logger.atWarn()
                    .addKeyValue(OperatorLoggingKeys.KIND, ResourcesUtil.kind(resource))
                    .addKeyValue(OperatorLoggingKeys.NAME, ResourcesUtil.name(resource))
                    .addKeyValue(OperatorLoggingKeys.NAMESPACE, ResourcesUtil.namespace(resource))
                    .log(message);
        }
        addCondition(type, message);
    }

    /**
     * Removes the log cache entry for the given key, so that the next call to
     * {@link #addConditionAndLogWarning} with this key will emit a fresh warning.
     *
     * @param logMessageCacheKey the cache key to invalidate
     */
    public void invalidateLogCacheEntry(String logMessageCacheKey) {
        LOG_CACHE.invalidate(logMessageCacheKey);
    }

    /**
     * Returns an unmodifiable view of the conditions accumulated by checkers so far.
     *
     * @return accumulated conditions
     */
    public List<Condition> conditions() {
        return List.copyOf(conditions);
    }
}