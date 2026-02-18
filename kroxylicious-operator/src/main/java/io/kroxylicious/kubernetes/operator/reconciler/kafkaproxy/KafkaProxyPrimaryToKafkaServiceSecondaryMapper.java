/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.reconciler.kafkaproxy;

import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import io.javaoperatorsdk.operator.processing.event.source.PrimaryToSecondaryMapper;

import io.kroxylicious.kubernetes.api.common.LocalRef;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaService;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaClusterSpec;
import io.kroxylicious.kubernetes.operator.ResourcesUtil;

import static io.kroxylicious.kubernetes.operator.ResourcesUtil.toLocalRef;

class KafkaProxyPrimaryToKafkaServiceSecondaryMapper implements PrimaryToSecondaryMapper<KafkaProxy> {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProxyPrimaryToKafkaServiceSecondaryMapper.class);

    private final EventSourceContext<KafkaProxy> context;

    KafkaProxyPrimaryToKafkaServiceSecondaryMapper(EventSourceContext<KafkaProxy> context) {
        this.context = context;
    }

    @Override
    public Set<ResourceID> toSecondaryResourceIDs(KafkaProxy primary) {
        // Load all the virtual clusters for the KafkaProxy, then extract all the referenced KafkaService resource ids.
        Set<? extends LocalRef<KafkaService>> clusterRefs = ResourcesUtil.resourcesInSameNamespace(context, primary, VirtualKafkaCluster.class)
                .filter(vkc -> {
                    LocalRef<KafkaProxy> proxyRef = vkc.getSpec().getProxyRef();
                    return proxyRef.equals(toLocalRef(primary));
                })
                .map(VirtualKafkaCluster::getSpec)
                .map(VirtualKafkaClusterSpec::getTargetKafkaServiceRef)
                .collect(Collectors.toSet());

        Set<ResourceID> kafkaServiceRefs = ResourcesUtil.filteredResourceIdsInSameNamespace(context, primary, KafkaService.class,
                cluster -> clusterRefs.contains(toLocalRef(cluster)));
        LOGGER.debug("Event source KafkaService PrimaryToSecondaryMapper got {}", kafkaServiceRefs);
        return kafkaServiceRefs;
    }
}
