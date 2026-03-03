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
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngress;
import io.kroxylicious.kubernetes.operator.ResourcesUtil;

class KafkaProxyIngressSecondaryToKafkaProxyPrimaryMapper implements SecondaryToPrimaryMapper<KafkaProxyIngress> {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProxyIngressSecondaryToKafkaProxyPrimaryMapper.class);

    private final EventSourceContext<KafkaProxy> context;

    KafkaProxyIngressSecondaryToKafkaProxyPrimaryMapper(EventSourceContext<KafkaProxy> context) {
        this.context = context;
    }

    @Override
    public Set<ResourceID> toPrimaryResourceIDs(KafkaProxyIngress ingress) {
        // we do not want to trigger reconciliation of any proxy if the ingress has not been reconciled
        if (!ResourcesUtil.isStatusFresh(ingress)) {
            LOGGER.debug("Ignoring event from ingress with stale status: {}", ResourcesUtil.toLocalRef(ingress));
            return Set.of();
        }
        // we need to reconcile all proxies when a kafka proxy ingress changes in case the proxyRef is updated, we need to update
        // the previously referenced proxy too.
        Set<ResourceID> proxyIds = ResourcesUtil.filteredResourceIdsInSameNamespace(context, ingress, KafkaProxy.class, proxy -> true);
        LOGGER.debug("Event source KafkaProxyIngress SecondaryToPrimaryMapper got {}", proxyIds);
        return proxyIds;
    }
}
