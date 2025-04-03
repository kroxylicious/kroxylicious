/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.time.Clock;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.ErrorStatusUpdateControl;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import io.javaoperatorsdk.operator.processing.event.source.EventSource;

import io.kroxylicious.kubernetes.api.common.Condition;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaService;

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
    public UpdateControl<KafkaService> reconcile(KafkaService service, Context<KafkaService> context) {
        UpdateControl<KafkaService> uc = Conditions.newTrueConditionStatusPatch(clock, service, Condition.Type.Accepted);
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Completed reconciliation of {}/{}", namespace(service), name(service));
        }
        return uc;
    }

    @Override
    public List<EventSource<?, KafkaService>> prepareEventSources(EventSourceContext<KafkaService> context) {
        return io.javaoperatorsdk.operator.api.reconciler.Reconciler.super.prepareEventSources(context);
    }

    @Override
    public ErrorStatusUpdateControl<KafkaService> updateErrorStatus(KafkaService service, Context<KafkaService> context, Exception e) {
        // ResolvedRefs to UNKNOWN
        ErrorStatusUpdateControl<KafkaService> uc = Conditions.newUnknownConditionStatusPatch(clock, service, Condition.Type.Accepted, e);
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Completed reconciliation of {}/{} with error {}", namespace(service), name(service), e.toString());
        }
        return uc;
    }
}
