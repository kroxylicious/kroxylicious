/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.time.Clock;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.client.CustomResource;

import io.kroxylicious.kubernetes.api.common.Condition;
import io.kroxylicious.kubernetes.api.common.ConditionBuilder;

/**
 * Base factory for creating Kubernetes status conditions on custom resources.
 *
 * @param <R> the custom resource type this factory creates status patches for
 */
public abstract class StatusFactory<R extends CustomResource<?, ?>> {

    private final Clock clock;

    /**
     * Constructs a status factory using the given clock for condition timestamps.
     *
     * @param clock the clock to use for generating condition transition times
     */
    protected StatusFactory(Clock clock) {
        this.clock = clock;
    }

    /**
     * Creates a new condition builder pre-populated with the current timestamp and observed generation.
     *
     * @param observedGenerationSource the resource whose metadata generation is recorded in the condition
     * @return a pre-populated condition builder
     */
    public ConditionBuilder newConditionBuilder(HasMetadata observedGenerationSource) {
        var now = clock.instant();
        return new ConditionBuilder()
                .withLastTransitionTime(now)
                .withObservedGeneration(ResourcesUtil.generation(observedGenerationSource));
    }

    /**
     * Creates a condition with status TRUE, using the given type and message.
     *
     * @param observedGenerationSource the resource whose metadata generation is recorded in the condition
     * @param type the condition type
     * @param message a human-readable message describing the condition
     * @return a new condition with status TRUE
     */
    public Condition newTrueCondition(HasMetadata observedGenerationSource, Condition.Type type, String message) {
        return newConditionBuilder(observedGenerationSource)
                .withType(type)
                .withStatus(Condition.Status.TRUE)
                .withMessage(message)
                .withReason(type.name())
                .build();
    }

    /**
     * Creates a condition with status TRUE and an empty message.
     *
     * @param observedGenerationSource the resource whose metadata generation is recorded in the condition
     * @param type the condition type
     * @return a new condition with status TRUE
     */
    public Condition newTrueCondition(HasMetadata observedGenerationSource, Condition.Type type) {
        return newTrueCondition(observedGenerationSource, type, "");
    }

    /**
     * Creates a condition with status FALSE, indicating the condition is not met.
     *
     * @param observedGenerationSource the resource whose metadata generation is recorded in the condition
     * @param type the condition type
     * @param reason a machine-readable reason for the false status
     * @param message a human-readable message explaining why the condition is false
     * @return a new condition with status FALSE
     */
    public Condition newFalseCondition(
                                       HasMetadata observedGenerationSource,
                                       Condition.Type type,
                                       String reason,
                                       String message) {
        return newConditionBuilder(observedGenerationSource)
                .withType(type)
                .withStatus(Condition.Status.FALSE)
                .withReason(reason)
                .withMessage(message)
                .build();
    }

    /**
     * Creates a condition with status UNKNOWN, using the exception's class name as the reason and its message as the description.
     *
     * @param observedResource the resource whose metadata generation is recorded in the condition
     * @param type the condition type
     * @param e the exception that caused the unknown status
     * @return a new condition with status UNKNOWN
     */
    public Condition newUnknownCondition(HasMetadata observedResource, Condition.Type type, Exception e) {
        return newConditionBuilder(observedResource)
                .withType(type)
                .withStatus(Condition.Status.UNKNOWN)
                .withReason(e.getClass().getName())
                .withMessage(e.getMessage())
                .build();
    }

    /**
     * Creates a status patch with an unknown condition due to an exception.
     * @param observedProxy the observed resource
     * @param type the condition type
     * @param e the exception that caused the unknown status
     * @return a resource containing the status patch
     */
    public abstract R newUnknownConditionStatusPatch(R observedProxy,
                                                     Condition.Type type,
                                                     Exception e);

    /**
     * Creates a status patch with a false condition.
     * @param observedProxy the observed resource
     * @param type the condition type
     * @param reason the reason the condition is false
     * @param message a human-readable message
     * @return a resource containing the status patch
     */
    public abstract R newFalseConditionStatusPatch(R observedProxy,
                                                   Condition.Type type,
                                                   String reason,
                                                   String message);

    /**
     * Creates a status patch with a true condition.
     * @param observedProxy the observed resource
     * @param type the condition type
     * @param checksum the referent checksum associated with the condition
     * @return a resource containing the status patch
     */
    public abstract R newTrueConditionStatusPatch(R observedProxy,
                                                  Condition.Type type,
                                                  String checksum);
}
