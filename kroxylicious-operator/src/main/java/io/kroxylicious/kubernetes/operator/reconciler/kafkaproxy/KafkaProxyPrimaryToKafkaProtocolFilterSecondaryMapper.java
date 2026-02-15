/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.reconciler.kafkaproxy;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import io.javaoperatorsdk.operator.processing.event.source.PrimaryToSecondaryMapper;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.operator.ResourcesUtil;

import static io.kroxylicious.kubernetes.operator.ResourcesUtil.name;
import static io.kroxylicious.kubernetes.operator.ResourcesUtil.namespace;

class KafkaProxyPrimaryToKafkaProtocolFilterSecondaryMapper implements PrimaryToSecondaryMapper<KafkaProxy> {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProxyPrimaryToKafkaProtocolFilterSecondaryMapper.class);

    private final EventSourceContext<KafkaProxy> context;

    KafkaProxyPrimaryToKafkaProtocolFilterSecondaryMapper(EventSourceContext<KafkaProxy> context) {
        this.context = context;
    }

    public static Predicate<VirtualKafkaCluster> clusterReferences(KafkaProxy proxy) {
        return cluster -> name(proxy).equals(cluster.getSpec().getProxyRef().getName());
    }

    @Override
    public Set<ResourceID> toSecondaryResourceIDs(KafkaProxy proxy) {
        Set<ResourceID> filterReferences = ResourcesUtil.resourcesInSameNamespace(context, proxy, VirtualKafkaCluster.class)
                .filter(clusterReferences(proxy))
                .flatMap(cluster -> Optional.ofNullable(cluster.getSpec().getFilterRefs()).orElse(List.of()).stream())
                .map(filter -> new ResourceID(filter.getName(), namespace(proxy)))
                .collect(Collectors.toSet());
        LOGGER.debug("KafkaProxy {} has references to filters {}", ResourceID.fromResource(proxy), filterReferences);
        return filterReferences;
    }
}
