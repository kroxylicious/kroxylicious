/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.reconciler.virtualkafkacluster;

import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import io.javaoperatorsdk.operator.processing.event.source.SecondaryToPrimaryMapper;

import io.kroxylicious.kubernetes.api.common.TrustAnchorRef;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.api.v1alpha1.virtualkafkaclusterspec.ingresses.Tls;
import io.kroxylicious.kubernetes.operator.ResourcesUtil;

class ConfigMapJoinedToIngressTrustAnchorRefSecondaryToVirtualKafkaClusterPrimaryMapper implements SecondaryToPrimaryMapper<ConfigMap> {

    private final EventSourceContext<VirtualKafkaCluster> context;

    ConfigMapJoinedToIngressTrustAnchorRefSecondaryToVirtualKafkaClusterPrimaryMapper(EventSourceContext<VirtualKafkaCluster> context) {
        this.context = context;
    }

    @Override
    public Set<ResourceID> toPrimaryResourceIDs(ConfigMap configMap) {
        return ResourcesUtil.findReferrersMulti(context,
                configMap,
                VirtualKafkaCluster.class,
                cluster -> cluster.getSpec().getIngresses().stream()
                        .flatMap(ingress -> Optional.ofNullable(ingress.getTls()).stream())
                        .map(Tls::getTrustAnchorRef)
                        .filter(Objects::nonNull)
                        .map(TrustAnchorRef::getRef)
                        .toList());
    }
}
