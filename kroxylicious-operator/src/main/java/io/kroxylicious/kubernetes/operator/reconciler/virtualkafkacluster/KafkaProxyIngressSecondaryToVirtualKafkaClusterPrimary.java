/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.reconciler.virtualkafkacluster;

import java.util.Set;

import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import io.javaoperatorsdk.operator.processing.event.source.SecondaryToPrimaryMapper;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngress;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.api.v1alpha1.virtualkafkaclusterspec.Ingresses;
import io.kroxylicious.kubernetes.operator.ResourcesUtil;

class KafkaProxyIngressSecondaryToVirtualKafkaClusterPrimary implements SecondaryToPrimaryMapper<KafkaProxyIngress> {
    private final EventSourceContext<VirtualKafkaCluster> context;

    KafkaProxyIngressSecondaryToVirtualKafkaClusterPrimary(EventSourceContext<VirtualKafkaCluster> context) {
        this.context = context;
    }

    @Override
    public Set<ResourceID> toPrimaryResourceIDs(KafkaProxyIngress ingress) {
        if (!ResourcesUtil.isStatusFresh(ingress)) {
            VirtualKafkaClusterReconciler.logIgnoredEvent(ingress);
            return Set.of();
        }
        return ResourcesUtil.findReferrersMulti(context,
                ingress,
                VirtualKafkaCluster.class,
                cluster -> cluster.getSpec().getIngresses().stream().map(Ingresses::getIngressRef).toList());
    }
}
