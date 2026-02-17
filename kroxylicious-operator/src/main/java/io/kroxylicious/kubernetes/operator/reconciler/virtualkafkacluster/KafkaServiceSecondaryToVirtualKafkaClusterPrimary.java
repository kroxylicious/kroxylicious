/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.reconciler.virtualkafkacluster;

import java.util.Optional;
import java.util.Set;

import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import io.javaoperatorsdk.operator.processing.event.source.SecondaryToPrimaryMapper;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaService;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.operator.ResourcesUtil;

class KafkaServiceSecondaryToVirtualKafkaClusterPrimary implements SecondaryToPrimaryMapper<KafkaService> {
    private final EventSourceContext<VirtualKafkaCluster> context;

    KafkaServiceSecondaryToVirtualKafkaClusterPrimary(EventSourceContext<VirtualKafkaCluster> context) {
        this.context = context;
    }

    @Override
    public Set<ResourceID> toPrimaryResourceIDs(KafkaService service) {
        if (!ResourcesUtil.isStatusFresh(service)) {
            VirtualKafkaClusterReconciler.logIgnoredEvent(service);
            return Set.of();
        }
        return ResourcesUtil.findReferrers(context,
                service,
                VirtualKafkaCluster.class,
                cluster -> Optional.of(cluster.getSpec().getTargetKafkaServiceRef()));
    }
}
