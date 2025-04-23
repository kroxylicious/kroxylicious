/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.time.Clock;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

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
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngress;

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
    private final KafkaProxyIngressStatusFactory statusFactory;

    KafkaProxyIngressReconciler(Clock clock) {
        this.statusFactory = new KafkaProxyIngressStatusFactory(Objects.requireNonNull(clock));
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
                        ingress -> Optional.of(ingress.getSpec().getProxyRef())))
                .build();
        return List.of(new InformerEventSource<>(configuration, context));
    }

    @Override
    public UpdateControl<KafkaProxyIngress> reconcile(
            KafkaProxyIngress ingress,
            Context<KafkaProxyIngress> context)
            throws Exception {

        var proxyOpt = context.getSecondaryResource(KafkaProxy.class, PROXY_EVENT_SOURCE_NAME);
        LOGGER.debug("spec.proxyRef.name resolves to: {}", proxyOpt);

        UpdateControl<KafkaProxyIngress> updateControl;
        if (proxyOpt.isPresent()) {
            String checksum = MetadataChecksumGenerator.checksumFor(proxyOpt.get());
            updateControl = UpdateControl.patchResourceAndStatus(
                    statusFactory.newTrueConditionStatusPatch(
                            ingress,
                            Condition.Type.ResolvedRefs,
                            checksum));
        }
        else {
            updateControl = UpdateControl.patchStatus(statusFactory.newFalseConditionStatusPatch(
                    ingress,
                    Condition.Type.ResolvedRefs,
                    Condition.REASON_REFS_NOT_FOUND,
                    "KafkaProxy spec.proxyRef.name not found"));
        }
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Completed reconciliation of {}/{}", namespace(ingress), name(ingress));
        }
        return updateControl;
    }

    @Override
    public ErrorStatusUpdateControl<KafkaProxyIngress> updateErrorStatus(
                                                                         KafkaProxyIngress ingress,
                                                                         Context<KafkaProxyIngress> context,
                                                                         Exception e) {
        // ResolvedRefs to UNKNOWN
        ErrorStatusUpdateControl<KafkaProxyIngress> uc = ErrorStatusUpdateControl
                .patchStatus(statusFactory.newUnknownConditionStatusPatch(ingress, Condition.Type.ResolvedRefs, e));
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Completed reconciliation of {}/{} with error {}", namespace(ingress), name(ingress), e.toString());
        }
        return uc;
    }
}
