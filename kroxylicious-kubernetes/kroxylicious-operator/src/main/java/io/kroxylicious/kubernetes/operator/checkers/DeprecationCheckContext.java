/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.checkers;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.slf4j.Logger;

import io.fabric8.kubernetes.client.CustomResource;

import io.kroxylicious.kubernetes.api.common.Condition;
import io.kroxylicious.kubernetes.operator.OperatorLoggingKeys;
import io.kroxylicious.kubernetes.operator.ResourcesUtil;
import io.kroxylicious.kubernetes.operator.StatusFactory;

/**
 * Carries the inputs and outputs for a single deprecation check pass.
 * <p>
 * Checkers call {@link #addConditionAndLogWarning} to simultaneously append a {@link Condition}
 * and log a warning. If a warning has already been logged recently (determined using
 * {@link #existingConditions} and {@link Condition#getLastTransitionTime()}) it won't be logged
 * again for a while.
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

    private static final Duration LOG_SUPPRESSION_WINDOW = Duration.ofHours(1);

    private final R resource;
    private final Logger logger;
    private final F statusFactory;
    private final List<Condition> existingConditions;
    private final List<Condition> conditions = new ArrayList<>();
    private final Clock clock;

    public DeprecationCheckContext(R resource, Logger logger, F statusFactory, Clock clock, List<Condition> existingConditions) {
        this.resource = resource;
        this.logger = logger;
        this.statusFactory = statusFactory;
        this.existingConditions = existingConditions;
        this.clock = clock;
    }

    /**
     * Returns the custom resource being checked.
     *
     * @return the custom resource
     */
    public R resource() {
        return resource;
    }

    private Optional<Condition> findExistingCondition(Condition.Type type, String message) {
        return existingConditions.stream()
                .filter(c -> type.equals(c.getType()))
                .filter(c -> message.equals(c.getMessage()))
                .filter(c -> Duration.between(c.getLastTransitionTime(), clock.instant()).compareTo(LOG_SUPPRESSION_WINDOW) < 0)
                .findFirst();
    }

    /**
     * @param type the condition type (typically {@link Condition.Type#DeprecationWarning})
     * @param message the human-readable message attached to the condition
     * @param shouldLog whether to log a warning or not
     *
     * @see #addConditionAndLogWarning(Condition.Type, String)
     */
    private void addCondition(Condition.Type type, String message, boolean shouldLog) {
        Optional<Condition> existingCondition = findExistingCondition(type, message);

        if (existingCondition.isEmpty() && shouldLog) {
            logger.atWarn()
                    .addKeyValue(OperatorLoggingKeys.KIND, ResourcesUtil.kind(resource))
                    .addKeyValue(OperatorLoggingKeys.NAME, ResourcesUtil.name(resource))
                    .addKeyValue(OperatorLoggingKeys.NAMESPACE, ResourcesUtil.namespace(resource))
                    .log(message);
        }

        existingCondition.ifPresentOrElse(conditions::add, () -> conditions.add(statusFactory.newTrueCondition(resource, type, message)));
    }

    /**
     * Same as {@link #addConditionAndLogWarning} but without logging.
     *
     * @param type the condition type (typically {@link Condition.Type#DeprecationWarning})
     * @param message the human-readable message attached to the condition
     */
    public void addCondition(Condition.Type type, String message) {
        addCondition(type, message, false);
    }

    /**
     * Appends a {@link Condition} to {@link #conditions()} and logs a warning.
     * <p>
     * If a similar {@link Condition} is already present in {@link #existingConditions}, no warning
     * is logged and the existing condition is appended to {@link #conditions()} instead, in order
     * to preserve {@link Condition#getLastTransitionTime()}.
     *
     * @param type the condition type (typically {@link Condition.Type#DeprecationWarning})
     * @param message the human-readable message that is logged and attached to the condition
     */
    public void addConditionAndLogWarning(Condition.Type type, String message) {
        addCondition(type, message, true);
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