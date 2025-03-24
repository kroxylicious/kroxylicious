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
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;

import static io.kroxylicious.kubernetes.operator.ResourcesUtil.name;
import static io.kroxylicious.kubernetes.operator.ResourcesUtil.namespace;

public final class VirtualKafkaClusterReconciler implements
        Reconciler<VirtualKafkaCluster> {

    private static final Logger LOGGER = LoggerFactory.getLogger(VirtualKafkaClusterReconciler.class);
    public static final String PROXY_EVENT_SOURCE_NAME = "proxy";

    private final Clock clock;

    public VirtualKafkaClusterReconciler(Clock clock) {
        this.clock = clock;
    }

    @Override
    public UpdateControl<VirtualKafkaCluster> reconcile(VirtualKafkaCluster cluster, Context<VirtualKafkaCluster> context) {
        var proxyOpt = context.getSecondaryResource(KafkaProxy.class, PROXY_EVENT_SOURCE_NAME);
        LOGGER.debug("spec.proxyRef.name resolves to: {}", proxyOpt);

        Condition condition;
        if (proxyOpt.isPresent()) {
            condition = ResourcesUtil.newResolvedRefsTrue(clock, cluster);
        }
        else {
            condition = ResourcesUtil.newResolvedRefsFalse(clock,
                    cluster,
                    "spec.proxyRef.name",
                    "KafkaProxy not found");
        }

        UpdateControl<VirtualKafkaCluster> uc = UpdateControl.patchStatus(ResourcesUtil.patchWithCondition(cluster, condition));
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Completed reconciliation of {}/{}", namespace(cluster), name(cluster));
        }
        return uc;
    }

    @Override
    public List<EventSource<?, VirtualKafkaCluster>> prepareEventSources(EventSourceContext<VirtualKafkaCluster> context) {
        InformerEventSourceConfiguration<KafkaProxy> configuration = InformerEventSourceConfiguration.from(
                KafkaProxy.class,
                VirtualKafkaCluster.class)
                .withName(PROXY_EVENT_SOURCE_NAME)
                .withPrimaryToSecondaryMapper((VirtualKafkaCluster cluster) -> ResourcesUtil.localRefAsResourceId(cluster, cluster.getSpec().getProxyRef()))
                .withSecondaryToPrimaryMapper(proxy -> ResourcesUtil.findReferrers(context,
                        proxy,
                        VirtualKafkaCluster.class,
                        cluster -> cluster.getSpec().getProxyRef()))
                .build();
        return List.of(new InformerEventSource<>(configuration, context));
    }

    @Override
    public ErrorStatusUpdateControl<VirtualKafkaCluster> updateErrorStatus(VirtualKafkaCluster cluster, Context<VirtualKafkaCluster> context, Exception e) {
        // ResolvedRefs to UNKNOWN
        Condition condition = ResourcesUtil.newUnknownCondition(clock, cluster, Condition.Type.ResolvedRefs, e);
        ErrorStatusUpdateControl<VirtualKafkaCluster> uc = ErrorStatusUpdateControl.patchStatus(ResourcesUtil.patchWithCondition(cluster, condition));
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Completed reconciliation of {}/{} for error {}", namespace(cluster), name(cluster), e.toString());
        }
        return uc;
    }
}
