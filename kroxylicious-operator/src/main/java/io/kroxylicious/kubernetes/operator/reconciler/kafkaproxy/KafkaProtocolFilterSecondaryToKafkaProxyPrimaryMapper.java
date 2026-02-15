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

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProtocolFilter;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.operator.ResourcesUtil;

class KafkaProtocolFilterSecondaryToKafkaProxyPrimaryMapper implements SecondaryToPrimaryMapper<KafkaProtocolFilter> {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProtocolFilterSecondaryToKafkaProxyPrimaryMapper.class);

    private final EventSourceContext<KafkaProxy> context;

    KafkaProtocolFilterSecondaryToKafkaProxyPrimaryMapper(EventSourceContext<KafkaProxy> context) {
        this.context = context;
    }

    @Override
    public Set<ResourceID> toPrimaryResourceIDs(KafkaProtocolFilter filter) {
        // we do not want to trigger reconciliation of any proxy if the filter has not been reconciled
        if (!ResourcesUtil.isStatusFresh(filter)) {
            LOGGER.debug("Ignoring event from filter with stale status: {}", ResourcesUtil.toLocalRef(filter));
            return Set.of();
        }
        // filters don't point to a proxy, but must be in the same namespace as the proxy/proxies which reference the,
        // so when a filter changes we reconcile all the proxies in the same namespace
        Set<ResourceID> proxiesInFilterNamespace = ResourcesUtil.filteredResourceIdsInSameNamespace(context, filter, KafkaProxy.class, proxy -> true);
        LOGGER.debug("Event source SecondaryToPrimaryMapper got {}", proxiesInFilterNamespace);
        return proxiesInFilterNamespace;
    }
}
