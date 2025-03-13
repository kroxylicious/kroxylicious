/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.ingress;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.fabric8.kubernetes.api.model.ContainerPort;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;

import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.proxy.config.VirtualClusterGateway;

import static io.kroxylicious.kubernetes.operator.ResourcesUtil.name;

/**
 * The ProxyIngressModel represents the logical arrangement of ingresses for a single KafkaProxy.
 * Different ingresses may need different resources on the pod, like unique identifying ports that
 * the proxy can use to determine the upstream nodes, or a shared port.
 * This also is a place where we can transform a single KafkaProxyIngress specification into multiple
 * Ingress instances (numerous virtual clusters can refer to the same ingress), with it own unique
 * resource allocation in the proxy pod.
 * It also describes invalid ingresses
 * @param clusters the list of cluster models, note that we do not consider if the clusters are broken yet
 */
public record ProxyIngressModel(List<VirtualClusterIngressModel> clusters) {

    public Optional<VirtualClusterIngressModel> clusterIngressModel(VirtualKafkaCluster cluster) {
        return clusters.stream()
                .filter(c -> name(c.cluster).equals(name(cluster)))
                .findFirst();
    }

    public record VirtualClusterIngressModel(VirtualKafkaCluster cluster, List<IngressModel> ingressModels) {
        public List<VirtualClusterGateway> gateways() {
            return ingressModels.stream().map(ingressModel -> ingressModel.ingressInstance.gatewayConfig()).toList();
        }

        public Stream<Service> services() {
            return ingressModels.stream().flatMap(it -> it.ingressInstance().services()).map(ServiceBuilder::build);
        }

        public Set<IngressConflictException> ingressExceptions() {
            return ingressModels.stream().flatMap(it -> Stream.ofNullable(it.exception)).collect(Collectors.toSet());
        }
    }

    public record IngressModel(IngressInstance ingressInstance, IngressConflictException exception) {

        public Stream<ContainerPort> proxyContainerPorts() {
            return ingressInstance.proxyContainerPorts();
        }

    }

}
