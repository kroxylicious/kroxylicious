/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.model.networking;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.fabric8.kubernetes.api.model.ContainerPort;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;

import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;

import edu.umd.cs.findbugs.annotations.Nullable;

import static io.kroxylicious.kubernetes.operator.ResourcesUtil.name;

/**
 * The ProxyNetworkingModel models the logical arrangement of client-facing resources, and backend plumbing
 * for a single KafkaProxy.
 * Different ingresses need different client-facing resources, like ClusterIP Services for on-cluster access
 * or LoadBalancer Services for off-cluster access.
 * Different ingresses may need different resources on the pod, like unique identifying ports that
 * the proxy can use to determine the upstream nodes, or a shared port for SNI access.
 * It also describes logical issues discovered when composing the proxy ingress model, such as conflicting ingresses that
 * required clashing resources.
 * @param clusterNetworkingModels the list of cluster models, note that we do not consider if the clusters are broken yet
 */
public record ProxyNetworkingModel(List<ClusterNetworkingModel> clusterNetworkingModels) {

    public Optional<ClusterNetworkingModel> clusterIngressModel(VirtualKafkaCluster cluster) {
        return clusterNetworkingModels.stream()
                .filter(c -> name(c.cluster).equals(name(cluster)))
                .findFirst();
    }

    /**
     *
     * @param cluster cluster
     * @param clusterIngressNetworkingModelResults the ingress model results, one per ingress in the VKC spec.ingresses in the same order
     */
    public record ClusterNetworkingModel(VirtualKafkaCluster cluster, List<ClusterIngressNetworkingModelResult> clusterIngressNetworkingModelResults) {

        public Stream<Service> services() {
            return clusterIngressNetworkingModelResults.stream().flatMap(it -> it.clusterIngressNetworkingModel().services()).map(ServiceBuilder::build);
        }

        public Set<IngressConflictException> ingressExceptions() {
            return clusterIngressNetworkingModelResults.stream()
                    .filter(it -> it.exception != null)
                    .map(ClusterIngressNetworkingModelResult::exception)
                    .collect(Collectors.toSet());
        }

        /**
         * Register the container ports of all ClusterIngressNetworkingModelResults
         * @param portConsumer consumer that will accept all container ports
         */
        public void registerProxyContainerPorts(Consumer<ContainerPort> portConsumer) {
            clusterIngressNetworkingModelResults.forEach(result -> {
                result.proxyContainerPorts().forEach(portConsumer);
            });
        }

        public boolean anyIngressRequiresSharedSniPort() {
            return clusterIngressNetworkingModelResults.stream()
                    .anyMatch(ingressModelResult -> ingressModelResult.clusterIngressNetworkingModel().requiresSharedSniContainerPort());
        }

        public Stream<Integer> requiredSniLoadbalancerPorts() {
            return clusterIngressNetworkingModelResults.stream()
                    .flatMap(ingressModelResult -> ingressModelResult.clusterIngressNetworkingModel().sharedLoadBalancerServiceRequirements().stream())
                    .flatMap(SharedLoadBalancerServiceRequirements::requiredClientFacingPorts);
        }
    }

    /**
     * The result of modelling a single ingress from a VKC spec.ingresses array into a ClusterIngressNetworkingModelResult
     * @param clusterIngressNetworkingModel the ingress model
     * @param exception an exception if there was a conflict, null otherwise
     */
    public record ClusterIngressNetworkingModelResult(ClusterIngressNetworkingModel clusterIngressNetworkingModel, @Nullable IngressConflictException exception) {

        public Stream<ContainerPort> proxyContainerPorts() {
            return clusterIngressNetworkingModel.identifyingProxyContainerPorts();
        }

    }

}
