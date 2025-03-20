/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.kafkaservice;

import java.time.Clock;
import java.util.List;

import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.ErrorStatusUpdateControl;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import io.javaoperatorsdk.operator.processing.event.source.EventSource;

import io.kroxylicious.kubernetes.api.common.Condition;
import io.kroxylicious.kubernetes.api.common.ConditionBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaService;

public final class KafkaServiceReconciler implements
        io.javaoperatorsdk.operator.api.reconciler.Reconciler<KafkaService> {

    private final Clock clock;

    public KafkaServiceReconciler(Clock clock) {
        this.clock = clock;
    }

    @Override
    public UpdateControl<KafkaService> reconcile(KafkaService resource, Context<KafkaService> context) {
        final Condition acceptedCondition = new ConditionBuilder()
                .withType(Condition.Type.Accepted)
                .withLastTransitionTime(clock.instant().atZone(clock.getZone()))
                .withObservedGeneration(resource.getMetadata().getGeneration())
                .withStatus(Condition.Status.TRUE)
                .build();
        final KafkaService amended = resource
                .edit()
                .editOrNewStatus()
                .addNewConditionLike(acceptedCondition)
                .endCondition()
                .endStatus()
                .build();
        return UpdateControl.patchStatus(amended);
    }

    @Override
    public List<EventSource<?, KafkaService>> prepareEventSources(EventSourceContext<KafkaService> context) {
        return io.javaoperatorsdk.operator.api.reconciler.Reconciler.super.prepareEventSources(context);
    }

    @Override
    public ErrorStatusUpdateControl<KafkaService> updateErrorStatus(KafkaService resource, Context<KafkaService> context, Exception e) {
        return io.javaoperatorsdk.operator.api.reconciler.Reconciler.super.updateErrorStatus(resource, context, e);
    }
}
