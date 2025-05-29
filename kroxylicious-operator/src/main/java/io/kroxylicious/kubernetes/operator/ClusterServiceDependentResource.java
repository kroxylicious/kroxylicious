/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.kubernetes.operator;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.processing.dependent.BulkDependentResource;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.CRUDKubernetesDependentResource;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.KubernetesDependent;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.operator.model.networking.ProxyNetworkingModel;
import io.kroxylicious.kubernetes.operator.resolver.ClusterResolutionResult;

import static io.kroxylicious.kubernetes.operator.Labels.standardLabels;
import static io.kroxylicious.kubernetes.operator.ProxyDeploymentDependentResource.SHARED_SNI_PORT;
import static io.kroxylicious.kubernetes.operator.ResourcesUtil.namespace;
import static io.kroxylicious.kubernetes.operator.ResourcesUtil.toByNameMap;

/**
 * Generates the Kube {@code Service} for a single virtual cluster.
 * This is named like {@code ${cluster.name}}, which allows clusters to migrate between proxy
 * instances in the same namespace without impacts clients using the Service's DNS name.
 */
@KubernetesDependent
public class ClusterServiceDependentResource
        extends CRUDKubernetesDependentResource<Service, KafkaProxy>
        implements BulkDependentResource<Service, KafkaProxy> {

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

        var serviceStream = clusterNetworkingModels.stream()
                .flatMap(ProxyNetworkingModel.ClusterNetworkingModel::services);

        var sharedSniLoadbalancerPorts = clusterNetworkingModels.stream()
                .flatMap(ProxyNetworkingModel.ClusterNetworkingModel::requiredSniLoadbalancerPorts)
                .distinct().sorted().toList();

        var sniServiceStream = sniLoadbalancerServices(primary, sharedSniLoadbalancerPorts);

        return Stream.concat(serviceStream, sniServiceStream).collect(toByNameMap());
    }

    private ObjectMeta serviceMetadata(KafkaProxy primary, String name) {
        return new ObjectMetaBuilder()
                .withName(name)
                .withNamespace(namespace(primary))
                .addToLabels(standardLabels(primary))
                .addNewOwnerReferenceLike(ResourcesUtil.newOwnerReferenceTo(primary)).endOwnerReference()
                .build();
    }

    private Stream<Service> sniLoadbalancerServices(KafkaProxy primary, List<Integer> loadBalancerPorts) {
        if (loadBalancerPorts.isEmpty()) {
            return Stream.empty();
        }
        else {
            String serviceName = ResourcesUtil.name(primary) + "-sni";
            var serviceSpecBuilder = new ServiceBuilder()
                    .withMetadata(serviceMetadata(primary, serviceName))
                    .withNewSpec()
                    .withType("LoadBalancer")
                    .withSelector(ProxyDeploymentDependentResource.podLabels(primary));
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
    }

    @Override
    public Map<String, Service> getSecondaryResources(
                                                      KafkaProxy primary,
                                                      Context<KafkaProxy> context) {
        Set<Service> secondaryResources = context.eventSourceRetriever().getEventSourceFor(Service.class)
                .getSecondaryResources(primary);
        return secondaryResources.stream().collect(toByNameMap());
    }
}
