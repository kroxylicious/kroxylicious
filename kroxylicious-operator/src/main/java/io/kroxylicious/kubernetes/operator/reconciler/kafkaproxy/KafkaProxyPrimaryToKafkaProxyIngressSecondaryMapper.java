/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.reconciler.kafkaproxy;

import java.util.Set;
import java.util.function.Predicate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import io.javaoperatorsdk.operator.processing.event.source.PrimaryToSecondaryMapper;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngress;
import io.kroxylicious.kubernetes.operator.ResourcesUtil;

import static io.kroxylicious.kubernetes.operator.ResourcesUtil.name;

class KafkaProxyPrimaryToKafkaProxyIngressSecondaryMapper implements PrimaryToSecondaryMapper<KafkaProxy> {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProxyPrimaryToKafkaProxyIngressSecondaryMapper.class);

    private final EventSourceContext<KafkaProxy> context;

    KafkaProxyPrimaryToKafkaProxyIngressSecondaryMapper(EventSourceContext<KafkaProxy> context) {
        this.context = context;
    }

    @Override
    public Set<ResourceID> toSecondaryResourceIDs(KafkaProxy primary) {
        Set<ResourceID> ingressesInProxyNamespace = ResourcesUtil.filteredResourceIdsInSameNamespace(context, primary, KafkaProxyIngress.class,
                ingressReferences(primary));
        LOGGER.debug("Event source KafkaProxyIngress PrimaryToSecondaryMapper got {}", ingressesInProxyNamespace);
        return ingressesInProxyNamespace;
    }

    private static Predicate<KafkaProxyIngress> ingressReferences(KafkaProxy proxy) {
        return ingress -> name(proxy).equals(ingress.getSpec().getProxyRef().getName());
    }
}
