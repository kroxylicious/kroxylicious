/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.reconciler.virtualkafkacluster;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.javaoperatorsdk.operator.processing.event.ResourceID;
import io.javaoperatorsdk.operator.processing.event.source.PrimaryToSecondaryMapper;

import io.kroxylicious.kubernetes.api.common.AnyLocalRefBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.api.v1alpha1.virtualkafkaclusterspec.Ingresses;
import io.kroxylicious.kubernetes.operator.ResourcesUtil;

import static io.kroxylicious.kubernetes.operator.model.networking.TcpClusterIPClusterIngressNetworkingModel.bootstrapServiceName;

class VirtualKafkaClusterPrimaryToKubernetesServicesSecondaryMapper implements PrimaryToSecondaryMapper<VirtualKafkaCluster> {
    @Override
    public Set<ResourceID> toSecondaryResourceIDs(VirtualKafkaCluster cluster) {
        Stream<ResourceID> clusterIpServices = cluster.getSpec().getIngresses()
                .stream()
                .map(Ingresses::getIngressRef)
                .flatMap(ir -> ResourcesUtil
                        .localRefAsResourceId(cluster, new AnyLocalRefBuilder().withName(bootstrapServiceName(cluster, ir.getName())).build()).stream());
        Stream<ResourceID> loadbalancerService = ResourcesUtil.localRefAsResourceId(cluster,
                new AnyLocalRefBuilder().withName(cluster.getSpec().getProxyRef().getName() + "-sni").build()).stream();
        return Stream.concat(clusterIpServices, loadbalancerService).collect(Collectors.toSet());
    }
}
