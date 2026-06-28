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
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaService;
import io.kroxylicious.kubernetes.operator.ResourcesUtil;

import static io.kroxylicious.kubernetes.operator.ResourcesUtil.findAllKnownPrimariesInNamespace;

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
            LOGGER.atDebug()
                    .addKeyValue("kafkaService", ResourcesUtil.toLocalRef(kafkaService))
                    .log("Ignoring event from KafkaService with stale status");
            return Set.of();
        }
        // reconcile all proxies in the namespace; the extra reconciliations for proxies that don't reference this
        // KafkaService are harmless no-ops, and this avoids API-server calls that can fail transiently (#4017)
        Set<ResourceID> proxyIds = findAllKnownPrimariesInNamespace(context, kafkaService);
        LOGGER.atDebug()
                .addKeyValue("proxyIds", proxyIds)
                .log("Event source KafkaService SecondaryToPrimaryMapper");
        return proxyIds;
    }
}
