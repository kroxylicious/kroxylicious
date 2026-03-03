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
import io.javaoperatorsdk.operator.processing.event.source.PrimaryToSecondaryMapper;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.operator.ResourcesUtil;

class KafkaProxyPrimaryToVirtualKafkaClusterSecondaryMapper implements PrimaryToSecondaryMapper<KafkaProxy> {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProxyPrimaryToVirtualKafkaClusterSecondaryMapper.class);

    private final EventSourceContext<KafkaProxy> context;

    KafkaProxyPrimaryToVirtualKafkaClusterSecondaryMapper(EventSourceContext<KafkaProxy> context) {
        this.context = context;
    }

    @Override
    public Set<ResourceID> toSecondaryResourceIDs(KafkaProxy proxy) {
        Set<ResourceID> virtualClustersInProxyNamespace = ResourcesUtil.filteredResourceIdsInSameNamespace(context, proxy, VirtualKafkaCluster.class,
                KafkaProxyPrimaryToKafkaProtocolFilterSecondaryMapper.clusterReferences(proxy));
        LOGGER.debug("Event source VirtualKafkaCluster PrimaryToSecondaryMapper got {}", virtualClustersInProxyNamespace);
        return virtualClustersInProxyNamespace;
    }
}
