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
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.processing.dependent.BulkDependentResource;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.CRUDKubernetesDependentResource;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.KubernetesDependent;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;

import static io.kroxylicious.kubernetes.operator.Labels.standardLabels;

/**
 * Generates the Kube {@code Service} for a single virtual cluster.
 * This is named like {@code ${cluster.name}}, which allows clusters to migrate between proxy
 * instances in the same namespace without impacts clients using the Service's DNS name.
 */
@KubernetesDependent
public class ClusterService
        extends CRUDKubernetesDependentResource<Service, KafkaProxy>
        implements BulkDependentResource<Service, KafkaProxy> {

    public ClusterService() {
        super(Service.class);
    }

    /**
     * @return The {@code metadata.name} of the desired {@code Service}.
     */
    static String serviceName(VirtualKafkaCluster cluster) {
        Objects.requireNonNull(cluster);
        return cluster.getMetadata().getName();
    }

    /**
     * @return the fully qualified service hostname
     */
    static String absoluteServiceHost(KafkaProxy primary, VirtualKafkaCluster cluster) {
        return serviceName(cluster) + "." + primary.getMetadata().getNamespace() + ".svc.cluster.local";
    }

    /**
     * The inverse of {@link #serviceName(VirtualKafkaCluster)}
     * @param service A service
     * @return  The name of the cluster corresponding to the given Service
     */
    static String clusterName(Service service) {
        return service.getMetadata().getName();
    }

    static Map<Integer, String> clusterPorts(Context<KafkaProxy> context, VirtualKafkaCluster cluster) {
        List<VirtualKafkaCluster> clusters = ResourcesUtil.clustersInNameOrder(context).toList();
        for (int clusterNum = 0; clusterNum < clusters.size(); clusterNum++) {
            if (clusters.get(clusterNum).getMetadata().getName().equals(cluster.getMetadata().getName())) {
                if (SharedKafkaProxyContext.isBroken(context, cluster)) {
                    return Map.of();
                }
                int startPort = 9292 + (100 * clusterNum);
                int numBrokerPorts = 4;
                return IntStream.range(startPort, startPort + numBrokerPorts).boxed()
                        .collect(Collectors.<Integer, Integer, String, TreeMap<Integer, String>> toMap(
                                portNum -> portNum,
                                portNum -> cluster.getMetadata().getName() + "-" + portNum,
                                (v1, v2) -> {
                                    throw new IllegalStateException();
                                },
                                TreeMap::new));
            }
        }
        throw new IllegalArgumentException("Couldn't find cluster with name " + cluster.getMetadata().getName());
    }

    protected Service clusterService(KafkaProxy primary,
                                     Context<KafkaProxy> context,
                                     VirtualKafkaCluster cluster) {
        // @formatter:off
        var serviceSpecBuilder = new ServiceBuilder()
                .withNewMetadata()
                    .withName(serviceName(cluster))
                    .withNamespace(primary.getMetadata().getNamespace())
                    .addToLabels(standardLabels(primary))
                    .addNewOwnerReferenceLike(ResourcesUtil.ownerReferenceTo(primary)).endOwnerReference()
                .endMetadata()
                .withNewSpec()
                    .withSelector(ProxyDeployment.podLabels(primary));
        for (var portNumEntry : clusterPorts( context, cluster).entrySet()) {
            serviceSpecBuilder = serviceSpecBuilder
                    .addNewPort()
                        .withName(portNumEntry.getValue())
                        .withPort(portNumEntry.getKey())
                        .withTargetPort(new IntOrString(portNumEntry.getKey()))
                        .withProtocol("TCP")
                    .endPort();
        }

        return serviceSpecBuilder
                .endSpec()
                .build();
        // @formatter:on
    }

    @Override
    public Map<String, Service> desiredResources(
                                                 KafkaProxy primary,
                                                 Context<KafkaProxy> context) {
        return ResourcesUtil.clustersInNameOrder(context)
                .filter(cluster -> !SharedKafkaProxyContext.isBroken(context, cluster))
                .collect(Collectors.toMap(
                        cluster -> cluster.getMetadata().getName(),
                        cluster -> clusterService(primary, context, cluster)));
    }

    @Override
    public Map<String, Service> getSecondaryResources(
                                                      KafkaProxy primary,
                                                      Context<KafkaProxy> context) {
        Set<Service> secondaryResources = context.eventSourceRetriever().getResourceEventSourceFor(Service.class)
                .getSecondaryResources(primary);
        return secondaryResources.stream()
                .collect(Collectors.toMap(
                        ClusterService::clusterName,
                        Function.identity()));
    }
}
