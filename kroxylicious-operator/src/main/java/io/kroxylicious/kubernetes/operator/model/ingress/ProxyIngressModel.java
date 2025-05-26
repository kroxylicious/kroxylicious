/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.model.ingress;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.fabric8.kubernetes.api.model.ContainerPort;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;

import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;

import edu.umd.cs.findbugs.annotations.Nullable;

import static io.kroxylicious.kubernetes.operator.ResourcesUtil.name;

/**
 * The ProxyIngressModel represents the logical arrangement of ingresses for a single KafkaProxy.
 * Different ingresses may need different resources on the pod, like unique identifying ports that
 * the proxy can use to determine the upstream nodes, or a shared port.
 * This also is a place where we can transform a single KafkaProxyIngress specification into multiple
 * IngressModel instances (numerous virtual clusters can refer to the same ingress), with it own unique
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

    /**
     *
     * @param cluster cluster
     * @param ingressModelResults the ingress model results, one per ingress in the VKC spec.ingresses in the same order
     */
    public record VirtualClusterIngressModel(VirtualKafkaCluster cluster, List<IngressModelResult> ingressModelResults) {

        public Stream<Service> services() {
            return ingressModelResults.stream().flatMap(it -> it.ingressModel().services()).map(ServiceBuilder::build);
        }

        public Set<IngressConflictException> ingressExceptions() {
            return ingressModelResults.stream().flatMap(it -> Stream.ofNullable(it.exception)).collect(Collectors.toSet());
        }

        public boolean requiresSharedTLSPort() {
            return ingressModelResults.stream().anyMatch(ingressModelResult -> ingressModelResult.ingressModel().requiresSharedTLSPort());
        }
    }

    /**
     * The result of modelling a single ingress from a VKC spec.ingresses array into an IngressModel
     * @param ingressModel the ingress model
     * @param exception an exception if there was a conflict, null otherwise
     */
    public record IngressModelResult(IngressModel ingressModel, @Nullable IngressConflictException exception) {

        public Stream<ContainerPort> proxyContainerPorts() {
            return ingressModel.proxyContainerPorts();
        }

    }

}
