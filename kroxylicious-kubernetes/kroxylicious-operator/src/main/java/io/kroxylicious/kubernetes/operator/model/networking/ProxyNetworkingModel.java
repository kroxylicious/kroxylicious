/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.model.networking;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.fabric8.kubernetes.api.model.ContainerPort;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.fabric8.openshift.api.model.Route;
import io.fabric8.openshift.api.model.RouteBuilder;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngress;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.operator.Annotations;
import io.kroxylicious.kubernetes.operator.ResourcesUtil;

import edu.umd.cs.findbugs.annotations.Nullable;

import static io.kroxylicious.kubernetes.operator.Labels.standardLabels;
import static io.kroxylicious.kubernetes.operator.ResourcesUtil.name;
import static io.kroxylicious.kubernetes.operator.ResourcesUtil.namespace;

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

    /**
     * Finds the networking model for a given cluster, if one exists.
     * @param cluster the virtual Kafka cluster to look up
     * @return an optional containing the cluster's networking model, or empty if not found
     */
    public Optional<ClusterNetworkingModel> clusterIngressModel(VirtualKafkaCluster cluster) {
        return clusterNetworkingModels.stream()
                .filter(c -> name(c.cluster).equals(name(cluster)))
                .findFirst();
    }

    /**
     * Builds every Kubernetes {@code Service} required by the clusters the caller considers valid: the per-cluster
     * ClusterIP and Route bootstrap Services, plus one shared {@code LoadBalancer} Service per referenced
     * {@code loadBalancer} {@code KafkaProxyIngress}.
     * <p>
     * The model deliberately retains broken clusters (so their ports stay stable if they are healed), so the
     * caller supplies {@code clusterHasValidNetworking} to exclude them; broken clusters must not contribute a
     * Service or any ports or bootstrap entries to one.
     *
     * @param primary the owning KafkaProxy
     * @param clusterHasValidNetworking predicate selecting the clusters whose models should contribute Services
     * @return a stream of all Services for the selected clusters
     */
    public Stream<Service> services(KafkaProxy primary, Predicate<VirtualKafkaCluster> clusterHasValidNetworking) {
        Stream<Service> perClusterServices = clusterNetworkingModels.stream()
                .filter(clusterNetworkingModel -> clusterHasValidNetworking.test(clusterNetworkingModel.cluster()))
                .flatMap(ClusterNetworkingModel::services);
        return Stream.concat(perClusterServices, sharedLoadBalancerServices(primary, clusterHasValidNetworking));
    }

    /**
     * Builds the shared {@code LoadBalancer} Services, one per {@code loadBalancer} {@code KafkaProxyIngress}
     * referenced by a cluster the caller considers valid. Because a single ingress is shared by many clusters,
     * its Service can only be assembled once the per-{@code (cluster, ingress)} models are grouped by ingress,
     * so the complete set of client-facing ports and bootstrap annotations is known. This is why the Service
     * is built here rather than by {@link ClusterIngressNetworkingModel#services()}.
     *
     * @param primary the owning KafkaProxy
     * @param clusterHasValidNetworking predicate selecting the clusters whose models should contribute to the Services
     * @return a stream of shared LoadBalancer Services, one per referenced loadBalancer ingress
     */
    private Stream<Service> sharedLoadBalancerServices(KafkaProxy primary, Predicate<VirtualKafkaCluster> clusterHasValidNetworking) {
        // Group the per-(cluster, ingress) models that require a shared LoadBalancer Service by the ingress
        // they belong to. A TreeMap keeps iteration order deterministic (golden-file tests depend on it)
        // irrespective of the order clusters/ingresses were discovered in.
        Map<String, List<ClusterIngressNetworkingModel>> modelsByIngressName = clusterNetworkingModels.stream()
                .filter(clusterNetworkingModel -> clusterHasValidNetworking.test(clusterNetworkingModel.cluster()))
                .flatMap(clusterNetworkingModel -> clusterNetworkingModel.clusterIngressNetworkingModelResults().stream())
                .map(ClusterIngressNetworkingModelResult::clusterIngressNetworkingModel)
                .filter(ingressModel -> ingressModel.sharedLoadBalancerServiceRequirements().isPresent())
                .collect(Collectors.groupingBy(ingressModel -> name(ingressModel.ingress()), TreeMap::new, Collectors.toList()));

        return modelsByIngressName.values().stream()
                .flatMap(ingressModels -> sharedLoadBalancerService(primary, ingressModels).stream());
    }

    private static Optional<Service> sharedLoadBalancerService(KafkaProxy primary, List<ClusterIngressNetworkingModel> ingressModels) {
        List<SharedLoadBalancerServiceRequirements> requirements = ingressModels.stream()
                .map(ingressModel -> ingressModel.sharedLoadBalancerServiceRequirements().orElseThrow())
                .toList();

        List<Integer> loadBalancerPorts = requirements.stream()
                .flatMap(SharedLoadBalancerServiceRequirements::requiredClientFacingPorts)
                .distinct()
                .sorted()
                .toList();
        if (loadBalancerPorts.isEmpty()) {
            return Optional.empty();
        }

        KafkaProxyIngress ingress = ingressModels.get(0).ingress();
        int targetPort = requirements.get(0).sharedSniTargetPort();
        Set<Annotations.ClusterIngressBootstrapServers> bootstraps = requirements.stream()
                .map(SharedLoadBalancerServiceRequirements::bootstrapServersToAnnotate)
                .collect(Collectors.toSet());

        ObjectMetaBuilder metadataBuilder = new ObjectMetaBuilder()
                .withName(name(ingress))
                .withNamespace(namespace(primary))
                .addToLabels(standardLabels(primary))
                .addNewOwnerReferenceLike(ResourcesUtil.newOwnerReferenceTo(primary)).endOwnerReference()
                .addNewOwnerReferenceLike(ResourcesUtil.newOwnerReferenceTo(ingress)).endOwnerReference();
        Annotations.annotateWithBootstrapServers(metadataBuilder, bootstraps);

        var serviceSpecBuilder = new ServiceBuilder()
                .withMetadata(metadataBuilder.build())
                .withNewSpec()
                .withType("LoadBalancer")
                .withSelector(standardLabels(primary));
        for (Integer loadBalancerPort : loadBalancerPorts) {
            serviceSpecBuilder = serviceSpecBuilder
                    .addNewPort()
                    .withName("sni-" + loadBalancerPort)
                    .withPort(loadBalancerPort)
                    .withTargetPort(new IntOrString(targetPort))
                    .withProtocol("TCP")
                    .endPort();
        }
        return Optional.of(serviceSpecBuilder.endSpec().build());
    }

    /**
     * The networking model for a single VirtualKafkaCluster, including all its ingresses.
     * @param cluster cluster
     * @param clusterIngressNetworkingModelResults the ingress model results, one per ingress in the VKC spec.ingresses in the same order
     */
    public record ClusterNetworkingModel(VirtualKafkaCluster cluster, List<ClusterIngressNetworkingModelResult> clusterIngressNetworkingModelResults) {

        /**
         * Builds all Kubernetes Services required by the ingresses of this cluster.
         * @return a stream of Services
         */
        public Stream<Service> services() {
            return clusterIngressNetworkingModelResults.stream().flatMap(it -> it.clusterIngressNetworkingModel().services()).map(ServiceBuilder::build);
        }

        /**
         * Builds all OpenShift Routes required by the ingresses of this cluster.
         * @return a stream of Routes
         */
        public Stream<Route> routes() {
            return clusterIngressNetworkingModelResults.stream().flatMap(it -> it.clusterIngressNetworkingModel().routes()).map(RouteBuilder::build);
        }

        /**
         * Collects all conflict exceptions encountered when modelling the cluster's ingresses.
         * @return the set of ingress conflict exceptions
         */
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
            clusterIngressNetworkingModelResults.forEach(result -> result.proxyContainerPorts().forEach(portConsumer));
        }

        /**
         * Determines whether any ingress in this cluster requires a shared SNI container port.
         * @return true if at least one ingress requires a shared SNI container port
         */
        public boolean anyIngressRequiresSharedSniPort() {
            return clusterIngressNetworkingModelResults.stream()
                    .anyMatch(ingressModelResult -> ingressModelResult.clusterIngressNetworkingModel().requiresSharedSniContainerPort());
        }
    }

    /**
     * The result of modelling a single ingress from a VKC spec.ingresses array into a ClusterIngressNetworkingModelResult
     * @param clusterIngressNetworkingModel the ingress model
     * @param exception an exception if there was a conflict, null otherwise
     */
    public record ClusterIngressNetworkingModelResult(ClusterIngressNetworkingModel clusterIngressNetworkingModel, @Nullable IngressConflictException exception) {

        /**
         * Returns the container ports that this ingress model requires on the proxy container.
         * @return a stream of container ports for identifying upstream nodes
         */
        public Stream<ContainerPort> proxyContainerPorts() {
            return clusterIngressNetworkingModel.identifyingProxyContainerPorts();
        }

    }

}
