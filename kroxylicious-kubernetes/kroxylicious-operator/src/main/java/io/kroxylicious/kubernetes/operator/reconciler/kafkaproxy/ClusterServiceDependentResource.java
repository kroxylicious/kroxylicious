/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.kubernetes.operator.reconciler.kafkaproxy;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.processing.dependent.BulkDependentResource;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.BooleanWithUndefined;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.CRUDKubernetesDependentResource;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.KubernetesDependent;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngress;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.operator.Annotations;
import io.kroxylicious.kubernetes.operator.ResourcesUtil;
import io.kroxylicious.kubernetes.operator.model.networking.ClusterIngressNetworkingModel;
import io.kroxylicious.kubernetes.operator.model.networking.ProxyNetworkingModel;
import io.kroxylicious.kubernetes.operator.resolver.ClusterResolutionResult;

import static io.kroxylicious.kubernetes.operator.Labels.standardLabels;
import static io.kroxylicious.kubernetes.operator.ResourcesUtil.namespace;
import static io.kroxylicious.kubernetes.operator.ResourcesUtil.toByNameMap;
import static io.kroxylicious.kubernetes.operator.reconciler.kafkaproxy.ProxyDeploymentDependentResource.SHARED_SNI_PORT;

/**
 * Generates the Kube {@code Service} for a single virtual cluster.
 * This is named like {@code ${cluster.name}}, which allows clusters to migrate between proxy
 * instances in the same namespace without impacts clients using the Service's DNS name.
 * Also generates one {@code Service} per {@code loadBalancer} {@code KafkaProxyIngress} that is
 * referenced by at least one valid {@code VirtualKafkaCluster}, named after that ingress.
 */
@KubernetesDependent(useSSA = BooleanWithUndefined.TRUE)
public class ClusterServiceDependentResource
        extends CRUDKubernetesDependentResource<Service, KafkaProxy>
        implements BulkDependentResource<Service, KafkaProxy, String> {

    /**
     * Constructs the dependent resource for managing Kubernetes Services.
     */
    public ClusterServiceDependentResource() {
        super(Service.class);
    }

    /**
     * @return The {@code metadata.name} of the desired {@code Service}.
     */
    static String serviceName(VirtualKafkaCluster cluster) {
        Objects.requireNonNull(cluster);
        return ResourcesUtil.name(cluster);
    }

    @Override
    public Map<String, Service> desiredResources(
                                                 KafkaProxy primary,
                                                 Context<KafkaProxy> context) {
        KafkaProxyContext kafkaProxyContext = KafkaProxyContext.proxyContext(context);
        var model = kafkaProxyContext.model();
        var clusterNetworkingModels = model.clustersWithValidNetworking().stream()
                .map(ClusterResolutionResult::cluster)
                .filter(cluster -> !kafkaProxyContext.isBroken(cluster))
                .flatMap(cluster -> model.networkingModel().clusterIngressModel(cluster).stream())
                .toList();

        var exclusiveServiceStream = clusterNetworkingModels.stream()
                .flatMap(ProxyNetworkingModel.ClusterNetworkingModel::services);

        // Group the per-(cluster, ingress) models by ingress name so that each ingress gets one Service.
        Collection<List<ClusterIngressNetworkingModel>> loadBalancerModelGroups = clusterNetworkingModels.stream()
                .flatMap(clusterNetworkingModel -> clusterNetworkingModel.clusterIngressNetworkingModelResults().stream())
                .map(ProxyNetworkingModel.ClusterIngressNetworkingModelResult::clusterIngressNetworkingModel)
                .filter(ingressModel -> ingressModel.sharedLoadBalancerServiceRequirements().isPresent())
                .collect(Collectors.groupingBy(ingressModel -> ResourcesUtil.name(ingressModel.ingress())))
                .values();

        var coalescedServiceStream = loadBalancerModelGroups.stream()
                .flatMap(ingressModels -> buildCoalescedServices(primary, ingressModels));

        return Stream.concat(exclusiveServiceStream, coalescedServiceStream).collect(toByNameMap());
    }

    private ObjectMeta coalescedServiceMetadata(KafkaProxy primary, KafkaProxyIngress ingress, String name,
                                                Set<Annotations.ClusterIngressBootstrapServers> bootstraps) {
        ObjectMetaBuilder builder = new ObjectMetaBuilder()
                .withName(name)
                .withNamespace(namespace(primary))
                .addToLabels(standardLabels(primary))
                .addNewOwnerReferenceLike(ResourcesUtil.newOwnerReferenceTo(primary)).endOwnerReference()
                .addNewOwnerReferenceLike(ResourcesUtil.newOwnerReferenceTo(ingress)).endOwnerReference();
        Annotations.annotateWithBootstrapServers(builder, bootstraps);
        return builder.build();
    }

    /**
     * Builds the single {@code Service} shared by one {@code loadBalancer} {@code KafkaProxyIngress}, given
     * all the per-cluster models that reference it.
     *
     * @param ingressModels the per-(cluster, ingress) models for a single ingress; every element must
     *         reference the same {@link KafkaProxyIngress}, which the caller guarantees by grouping models
     *         on ingress name before calling this method
     * @throws IllegalArgumentException if the models do not all reference the same {@code KafkaProxyIngress}
     */
    private Stream<Service> buildCoalescedServices(KafkaProxy primary, List<ClusterIngressNetworkingModel> ingressModels) {
        KafkaProxyIngress ingress = requireSameIngress(ingressModels);
        List<Integer> loadBalancerPorts = ingressModels.stream()
                .flatMap(ingressModel -> ingressModel.sharedLoadBalancerServiceRequirements().orElseThrow().requiredClientFacingPorts())
                .distinct()
                .sorted()
                .toList();
        if (loadBalancerPorts.isEmpty()) {
            return Stream.empty();
        }
        Set<Annotations.ClusterIngressBootstrapServers> bootstraps = ingressModels.stream()
                .map(ingressModel -> ingressModel.sharedLoadBalancerServiceRequirements().orElseThrow().bootstrapServersToAnnotate())
                .collect(Collectors.toSet());

        String serviceName = ResourcesUtil.name(ingress);
        var serviceSpecBuilder = new ServiceBuilder()
                .withMetadata(coalescedServiceMetadata(primary, ingress, serviceName, bootstraps))
                .withNewSpec()
                .withType("LoadBalancer")
                .withSelector(standardLabels(primary));
        for (Integer loadBalancerPort : loadBalancerPorts) {
            serviceSpecBuilder = serviceSpecBuilder
                    .addNewPort()
                    .withName("sni-" + loadBalancerPort)
                    .withPort(loadBalancerPort)
                    .withTargetPort(new IntOrString(SHARED_SNI_PORT))
                    .withProtocol("TCP")
                    .endPort();
        }
        return Stream.of(serviceSpecBuilder.endSpec().build());
    }

    private static KafkaProxyIngress requireSameIngress(List<ClusterIngressNetworkingModel> ingressModels) {
        Set<String> ingressNames = ingressModels.stream()
                .map(ingressModel -> ResourcesUtil.name(ingressModel.ingress()))
                .collect(Collectors.toSet());
        if (ingressNames.size() != 1) {
            throw new IllegalArgumentException("Expected all models to reference the same KafkaProxyIngress, got names: " + ingressNames);
        }
        return ingressModels.get(0).ingress();
    }

    @Override
    public Map<String, Service> getSecondaryResources(
                                                      KafkaProxy primary,
                                                      Context<KafkaProxy> context) {
        Set<Service> secondaryResources = context.eventSourceRetriever().getEventSourceFor(Service.class)
                .getSecondaryResources(primary);
        return secondaryResources.stream().collect(toByNameMap());
    }

    @Override
    public void deleteTargetResource(KafkaProxy primary, Service resource, String key, Context<KafkaProxy> context) {
        context.getClient().resource(resource).delete();
    }
}
