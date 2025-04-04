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

public abstract class StatusFactory<R extends CustomResource<?, ?>> {

    private final Clock clock;

    protected StatusFactory(Clock clock) {
        this.clock = clock;
    }

    ConditionBuilder newConditionBuilder(HasMetadata observedGenerationSource) {
        var now = clock.instant();
        return new ConditionBuilder()
                .withLastTransitionTime(now)
                .withObservedGeneration(observedGenerationSource.getMetadata().getGeneration());
    }

    Condition newTrueCondition(HasMetadata observedGenerationSource, Condition.Type type) {
        return newConditionBuilder(observedGenerationSource)
                .withType(type)
                .withStatus(Condition.Status.TRUE)
                .build();
    }

    Condition newFalseCondition(
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

    Condition newUnknownCondition(HasMetadata observedResource, Condition.Type type, Exception e) {
        return newConditionBuilder(observedResource)
                .withType(type)
                .withStatus(Condition.Status.UNKNOWN)
                .withReason(e.getClass().getName())
                .withMessage(e.getMessage())
                .build();
    }

    abstract R newUnknownConditionStatusPatch(R observedProxy,
                                              Condition.Type type,
                                              Exception e);

    abstract R newFalseConditionStatusPatch(R observedProxy,
                                            Condition.Type type,
                                            String reason,
                                            String message);

    abstract R newTrueConditionStatusPatch(R observedProxy,
                                           Condition.Type type);
}
