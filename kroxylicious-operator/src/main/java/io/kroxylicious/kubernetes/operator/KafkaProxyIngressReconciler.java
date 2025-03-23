/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.time.Clock;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.javaoperatorsdk.operator.api.config.informer.InformerEventSourceConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.ErrorStatusUpdateControl;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.api.reconciler.Reconciler;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import io.javaoperatorsdk.operator.processing.event.source.EventSource;
import io.javaoperatorsdk.operator.processing.event.source.informer.InformerEventSource;

import io.kroxylicious.kubernetes.api.common.Condition;
import io.kroxylicious.kubernetes.api.common.ConditionBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngress;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngressBuilder;

import edu.umd.cs.findbugs.annotations.NonNull;

import static io.kroxylicious.kubernetes.operator.ResourcesUtil.name;
import static io.kroxylicious.kubernetes.operator.ResourcesUtil.namespace;

/**
 * Reconciles a {@link KafkaProxyIngress} by checking whether the {@link KafkaProxy}
 * referenced by the {@code spec.proxyRef.name} actually exists, setting a
 * {@link Condition.Type#ResolvedRefs} {@link Condition} accordingly.
 */
public class KafkaProxyIngressReconciler implements
        Reconciler<KafkaProxyIngress> {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProxyIngressReconciler.class);
    public static final String PROXY_EVENT_SOURCE_NAME = "proxy";
    private final Clock clock;

    KafkaProxyIngressReconciler(Clock clock) {
        this.clock = clock;
    }

    @Override
    public List<EventSource<?, KafkaProxyIngress>> prepareEventSources(EventSourceContext<KafkaProxyIngress> context) {
        InformerEventSourceConfiguration<KafkaProxy> configuration = InformerEventSourceConfiguration.from(
                KafkaProxy.class,
                KafkaProxyIngress.class)
                .withName(PROXY_EVENT_SOURCE_NAME)
                .withPrimaryToSecondaryMapper((KafkaProxyIngress ingress) -> ResourcesUtil.localRefAsResourceId(ingress, ingress.getSpec().getProxyRef()))
                .withSecondaryToPrimaryMapper(proxy -> ResourcesUtil.findReferrers(context,
                        proxy,
                        KafkaProxyIngress.class,
                        ingress -> ingress.getSpec().getProxyRef()))
                .build();
        return List.of(new InformerEventSource<>(configuration, context));
    }

    @Override
    public UpdateControl<KafkaProxyIngress> reconcile(
                                                      KafkaProxyIngress ingress,
                                                      Context<KafkaProxyIngress> context)
            throws Exception {

        var now = ZonedDateTime.ofInstant(clock.instant(), ZoneId.of("Z"));

        ConditionBuilder conditionBuilder = newResolvedRefsCondition(ingress, now);

        var proxyOpt = context.getSecondaryResource(KafkaProxy.class, PROXY_EVENT_SOURCE_NAME);
        LOGGER.debug("spec.proxyRef.name resolves to: {}", proxyOpt);

        if (proxyOpt.isPresent()) {
            conditionBuilder.withStatus(Condition.Status.TRUE);
        }
        else {
            conditionBuilder.withStatus(Condition.Status.FALSE)
                    .withReason("spec.proxyRef.name")
                    .withMessage("KafkaProxy not found");
        }

        UpdateControl<KafkaProxyIngress> uc = UpdateControl.patchStatus(newIngressWithCondition(ingress, conditionBuilder.build()));
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Completed reconciliation of {}/{}", namespace(ingress), name(ingress));
        }
        return uc;
    }

    @NonNull
    private static KafkaProxyIngress newIngressWithCondition(KafkaProxyIngress ingress, Condition condition) {
        // @formatter:off
        return new KafkaProxyIngressBuilder()
                    .withNewMetadata()
                        .withName(ResourcesUtil.name(ingress))
                        .withNamespace(ResourcesUtil.namespace(ingress))
                        .withUid(ResourcesUtil.uid(ingress))
                    .endMetadata()
                    .withNewStatus()
                        .withObservedGeneration(ingress.getMetadata().getGeneration())
                        .withConditions(condition) // overwrite any existing conditions
                    .endStatus()
                .build();
        // @formatter:on
    }

    private static ConditionBuilder newResolvedRefsCondition(KafkaProxyIngress ingress, ZonedDateTime now) {
        return new ConditionBuilder()
                .withType(Condition.Type.ResolvedRefs)
                .withLastTransitionTime(now)
                .withObservedGeneration(ingress.getMetadata().getGeneration());
    }

    @Override
    public ErrorStatusUpdateControl<KafkaProxyIngress> updateErrorStatus(
                                                                         KafkaProxyIngress ingress,
                                                                         Context<KafkaProxyIngress> context,
                                                                         Exception e) {
        var now = ZonedDateTime.ofInstant(clock.instant(), ZoneId.of("Z"));
        // ResolvedRefs to UNKNOWN
        Condition condition = newResolvedRefsCondition(ingress, now)
                .withStatus(Condition.Status.UNKNOWN)
                .withReason(e.getClass().getName())
                .withMessage(e.getMessage())
                .build();
        ErrorStatusUpdateControl<KafkaProxyIngress> uc = ErrorStatusUpdateControl.patchStatus(
                newIngressWithCondition(ingress, condition));
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Completed reconciliation of {}/{} for error {}", namespace(ingress), name(ingress), e.toString());
        }
        return uc;
    }
}
