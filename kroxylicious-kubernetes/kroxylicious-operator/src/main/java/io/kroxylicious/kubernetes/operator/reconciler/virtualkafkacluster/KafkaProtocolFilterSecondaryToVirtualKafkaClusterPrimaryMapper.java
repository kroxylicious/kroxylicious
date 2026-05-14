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

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProtocolFilter;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.operator.ResourcesUtil;

class KafkaProtocolFilterSecondaryToVirtualKafkaClusterPrimaryMapper implements SecondaryToPrimaryMapper<KafkaProtocolFilter> {
    private final EventSourceContext<VirtualKafkaCluster> context;

    KafkaProtocolFilterSecondaryToVirtualKafkaClusterPrimaryMapper(EventSourceContext<VirtualKafkaCluster> context) {
        this.context = context;
    }

    @Override
    public Set<ResourceID> toPrimaryResourceIDs(KafkaProtocolFilter filter) {
        if (!ResourcesUtil.isStatusFresh(filter)) {
            VirtualKafkaClusterReconciler.logIgnoredEvent(filter);
            return Set.of();
        }
        return ResourcesUtil.findReferrersMulti(context,
                filter,
                VirtualKafkaCluster.class,
                cluster -> cluster.getSpec().getFilterRefs());
    }
}
