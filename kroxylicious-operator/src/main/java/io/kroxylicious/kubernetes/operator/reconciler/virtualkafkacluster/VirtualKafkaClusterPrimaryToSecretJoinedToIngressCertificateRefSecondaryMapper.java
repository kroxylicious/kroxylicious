/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.reconciler.virtualkafkacluster;

import java.util.Optional;
import java.util.Set;

import io.javaoperatorsdk.operator.processing.event.ResourceID;
import io.javaoperatorsdk.operator.processing.event.source.PrimaryToSecondaryMapper;

import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.api.v1alpha1.virtualkafkaclusterspec.ingresses.Tls;
import io.kroxylicious.kubernetes.operator.ResourcesUtil;

class VirtualKafkaClusterPrimaryToSecretJoinedToIngressCertificateRefSecondaryMapper implements PrimaryToSecondaryMapper<VirtualKafkaCluster> {

    @Override
    public Set<ResourceID> toSecondaryResourceIDs(VirtualKafkaCluster virtualKafkaCluster) {
        return ResourcesUtil.localRefsAsResourceIds(virtualKafkaCluster,
                virtualKafkaCluster.getSpec().getIngresses().stream()
                        .flatMap(ingress -> Optional.ofNullable(ingress.getTls()).stream())
                        .map(Tls::getCertificateRef)
                        .toList());
    }
}
