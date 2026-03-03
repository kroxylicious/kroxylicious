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
import io.javaoperatorsdk.operator.processing.event.source.SecondaryToPrimaryMapper;

import io.kroxylicious.kubernetes.api.common.LocalRef;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaService;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaClusterSpec;
import io.kroxylicious.kubernetes.operator.ResourcesUtil;

import static io.kroxylicious.kubernetes.operator.ResourcesUtil.toLocalRef;

class KafkaServiceSecondaryToKafkaProxyPrimaryMapper implements SecondaryToPrimaryMapper<KafkaService> {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaServiceSecondaryToKafkaProxyPrimaryMapper.class);

    private final EventSourceContext<KafkaProxy> context;

    KafkaServiceSecondaryToKafkaProxyPrimaryMapper(EventSourceContext<KafkaProxy> context) {
        this.context = context;
    }

    @Override
    public Set<ResourceID> toPrimaryResourceIDs(KafkaService kafkaService) {
        // we do not want to trigger reconciliation of any proxy if the ingress has not been reconciled
        if (!ResourcesUtil.isStatusFresh(kafkaService)) {
            LOGGER.debug("Ignoring event from KafkaService with stale status: {}", ResourcesUtil.toLocalRef(kafkaService));
            return Set.of();
        }
        // find all virtual clusters that reference this kafkaServiceRef

        Set<? extends LocalRef<KafkaProxy>> proxyRefs = ResourcesUtil.resourcesInSameNamespace(context, kafkaService, VirtualKafkaCluster.class)
                .filter(vkc -> vkc.getSpec().getTargetKafkaServiceRef().equals(ResourcesUtil.toLocalRef(kafkaService)))
                .map(VirtualKafkaCluster::getSpec)
                .map(VirtualKafkaClusterSpec::getProxyRef)
                .collect(Collectors.toSet());

        Set<ResourceID> proxyIds = ResourcesUtil.filteredResourceIdsInSameNamespace(context, kafkaService, KafkaProxy.class,
                proxy -> proxyRefs.contains(toLocalRef(proxy)));
        LOGGER.debug("Event source KafkaService SecondaryToPrimaryMapper got {}", proxyIds);
        return proxyIds;
    }
}
