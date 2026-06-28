/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.reconciler.virtualkafkacluster;

import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.javaoperatorsdk.operator.processing.event.ResourceID;
import io.javaoperatorsdk.operator.processing.event.source.PrimaryToSecondaryMapper;

import io.kroxylicious.kubernetes.api.common.AnyLocalRef;
import io.kroxylicious.kubernetes.api.common.AnyLocalRefBuilder;
import io.kroxylicious.kubernetes.api.common.IngressRef;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.api.v1alpha1.virtualkafkaclusterspec.Ingresses;
import io.kroxylicious.kubernetes.operator.ResourcesUtil;
import io.kroxylicious.kubernetes.operator.model.networking.RouteClusterIngressNetworkingModel;
import io.kroxylicious.kubernetes.operator.model.networking.TcpClusterIPClusterIngressNetworkingModel;

class VirtualKafkaClusterPrimaryToKubernetesServiceSecondaryMapper implements PrimaryToSecondaryMapper<VirtualKafkaCluster> {

    @Override
    public Set<ResourceID> toSecondaryResourceIDs(VirtualKafkaCluster cluster) {
        Stream<ResourceID> clusterIpServices = getIngressNameStream(cluster)
                .flatMap(ingressRefName -> ResourcesUtil.localRefAsResourceId(cluster,
                        buildAnyRef(TcpClusterIPClusterIngressNetworkingModel.bootstrapServiceName(cluster, ingressRefName))).stream());

        Stream<ResourceID> openShiftRouteServices = getIngressNameStream(cluster)
                .flatMap(ingressRefName -> ResourcesUtil
                        .localRefAsResourceId(cluster, buildAnyRef(RouteClusterIngressNetworkingModel.bootstrapServiceName(cluster, ingressRefName))).stream());

        Stream<ResourceID> loadbalancerService = ResourcesUtil.localRefAsResourceId(cluster,
                buildAnyRef(cluster.getSpec().getProxyRef().getName() + "-sni")).stream();
        return Stream.concat(Stream.concat(clusterIpServices, loadbalancerService), openShiftRouteServices).collect(Collectors.toSet());
    }

    private static AnyLocalRef buildAnyRef(String name) {
        return new AnyLocalRefBuilder().withName(name).build();
    }

    private static Stream<String> getIngressNameStream(VirtualKafkaCluster cluster) {
        return cluster.getSpec().getIngresses()
                .stream()
                .map(Ingresses::getIngressRef)
                .map(IngressRef::getName)
                .filter(Objects::nonNull);
    }
}
