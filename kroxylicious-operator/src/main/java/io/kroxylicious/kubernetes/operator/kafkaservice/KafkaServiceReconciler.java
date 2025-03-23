/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.kafkaservice;

import java.time.Clock;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.ErrorStatusUpdateControl;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import io.javaoperatorsdk.operator.processing.event.source.EventSource;

import io.kroxylicious.kubernetes.api.common.Condition;
import io.kroxylicious.kubernetes.api.common.ConditionBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaService;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaServiceBuilder;
import io.kroxylicious.kubernetes.operator.ResourcesUtil;

import static io.kroxylicious.kubernetes.operator.ResourcesUtil.name;
import static io.kroxylicious.kubernetes.operator.ResourcesUtil.namespace;

public final class KafkaServiceReconciler implements
        io.javaoperatorsdk.operator.api.reconciler.Reconciler<KafkaService> {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaServiceReconciler.class);

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
        UpdateControl<KafkaService> uc = UpdateControl.patchStatus(newServiceWithCondition(resource, acceptedCondition));
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Completed reconciliation of {}/{}", namespace(resource), name(resource));
        }
        return uc;
    }

    private static KafkaService newServiceWithCondition(KafkaService resource, Condition acceptedCondition) {
        // @formatter:off
        return new KafkaServiceBuilder()
                    .withNewMetadata()
                        .withName(ResourcesUtil.name(resource))
                        .withNamespace(ResourcesUtil.namespace(resource))
                        .withUid(ResourcesUtil.uid(resource))
                    .endMetadata()
                    .withNewStatus()
                        .withObservedGeneration(resource.getMetadata().getGeneration())
                        .withConditions(acceptedCondition)
                    .endStatus()
                .build();
        // @formatter:on
    }

    @Override
    public List<EventSource<?, KafkaService>> prepareEventSources(EventSourceContext<KafkaService> context) {
        return io.javaoperatorsdk.operator.api.reconciler.Reconciler.super.prepareEventSources(context);
    }

    @Override
    public ErrorStatusUpdateControl<KafkaService> updateErrorStatus(KafkaService resource, Context<KafkaService> context, Exception e) {
        var now = ZonedDateTime.ofInstant(clock.instant(), ZoneId.of("Z"));
        // ResolvedRefs to UNKNOWN
        Condition condition = new ConditionBuilder()
                .withType(Condition.Type.ResolvedRefs)
                .withLastTransitionTime(now)
                .withObservedGeneration(resource.getMetadata().getGeneration())
                .withStatus(Condition.Status.UNKNOWN)
                .withReason(e.getClass().getName())
                .withMessage(e.getMessage())
                .build();
        ErrorStatusUpdateControl<KafkaService> uc = ErrorStatusUpdateControl.patchStatus(newServiceWithCondition(resource, condition));
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Completed reconciliation of {}/{} for error {}", namespace(resource), name(resource), e.toString());
        }
        return uc;
    }
}
