/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.reconciler.virtualkafkacluster;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import io.javaoperatorsdk.operator.processing.event.ResourceID;
import io.javaoperatorsdk.operator.processing.event.source.PrimaryToSecondaryMapper;

import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaClusterSpec;
import io.kroxylicious.kubernetes.operator.ResourcesUtil;

class VirtualKafkaClusterPrimaryToKafkaProtocolFilterSecondaryMapper implements PrimaryToSecondaryMapper<VirtualKafkaCluster> {
    @Override
    public Set<ResourceID> toSecondaryResourceIDs(VirtualKafkaCluster cluster) {
        return ResourcesUtil.localRefsAsResourceIds(cluster,
                Optional.ofNullable(cluster.getSpec()).map(VirtualKafkaClusterSpec::getFilterRefs).orElse(List.of()));
    }
}
