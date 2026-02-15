/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.reconciler.kafkaproxy;

import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import io.javaoperatorsdk.operator.processing.event.source.SecondaryToPrimaryMapper;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.operator.ResourcesUtil;

public class VirtualKafkaClusterSecondaryToKafkaProxyPrimaryMapper implements SecondaryToPrimaryMapper<VirtualKafkaCluster> {
    private static final Logger LOGGER = LoggerFactory.getLogger(VirtualKafkaClusterSecondaryToKafkaProxyPrimaryMapper.class);

    private final EventSourceContext<KafkaProxy> context;

    public VirtualKafkaClusterSecondaryToKafkaProxyPrimaryMapper(EventSourceContext<KafkaProxy> context) {
        this.context = context;
    }

    @Override
    public Set<ResourceID> toPrimaryResourceIDs(VirtualKafkaCluster cluster) {
        // we do not want to trigger reconciliation of any proxy if the cluster has not been reconciled
        if (!ResourcesUtil.isStatusFresh(cluster)) {
            LOGGER.debug("Ignoring event from cluster with stale status: {}", ResourcesUtil.toLocalRef(cluster));
            return Set.of();
        }
        // we need to reconcile all proxies when a virtual kafka cluster changes in case the proxyRef is updated, we need to update
        // the previously referenced proxy too.
        Set<ResourceID> proxyIds = ResourcesUtil.filteredResourceIdsInSameNamespace(context, cluster, KafkaProxy.class, proxy -> true);
        LOGGER.debug("Event source VirtualKafkaCluster SecondaryToPrimaryMapper got {}", proxyIds);
        return proxyIds;
    }
}
