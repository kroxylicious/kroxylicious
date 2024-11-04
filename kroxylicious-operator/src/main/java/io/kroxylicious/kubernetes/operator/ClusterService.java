/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.kubernetes.operator;

import java.util.HashMap;
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
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaproxyspec.Clusters;

/**
 * The Kube {@code Service} for a single virtual cluster.
 * This is named like {@code ${cluster.name}-cluster-${KafkaProxy.metadata.name}}.
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
    static String serviceName(KafkaProxy primary, Clusters cluster) {
        Objects.requireNonNull(primary);
        Objects.requireNonNull(cluster);
        String serviceName = cluster.getName() + "-cluster-" + primary.getMetadata().getName();
        if (serviceName.length() > 63) {
            throw new SchemaValidatedInvalidResourceException(
                    "For each spec.cluster[], the total length of its name and the metadata.name must be at most 54 characters");
        }
        return serviceName;
    }

    static Map<Integer, String> clusterPorts(KafkaProxy primary, Clusters cluster) {
        var clusters = ClustersUtil.distinctClusters(primary);
        for (int clusterNum = 0; clusterNum < clusters.size(); clusterNum++) {
            if (clusters.get(clusterNum).getName().equals(cluster.getName())) {
                int startPort = 9292 + (100 * clusterNum);
                int numBrokerPorts = 4;
                return IntStream.range(startPort, startPort + numBrokerPorts).boxed()
                        .collect(Collectors.<Integer, Integer, String, TreeMap<Integer, String>> toMap(
                                portNum -> portNum,
                                portNum -> cluster.getName() + "-cluster-" + portNum,
                                (v1, v2) -> {
                                    throw new IllegalStateException();
                                },
                                TreeMap::new));
            }
        }
        throw new IllegalArgumentException("Couldn't find cluster with name " + cluster.getName());
    }

    protected Service clusterService(KafkaProxy primary,
                                     Clusters cluster) {
        // formatter=off
        var serviceSpecBuilder = new ServiceBuilder()
                .withNewMetadata()
                    .withName(serviceName(primary, cluster))
                    .withNamespace(primary.getMetadata().getNamespace())
                .endMetadata()
                .withNewSpec()
                    .withSelector(ProxyDeployment.podLabels());
        for (var portNumEntry : clusterPorts(primary, cluster).entrySet()) {
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
    }

    @Override
    public Map<String, Service> desiredResources(
                                                 KafkaProxy primary,
                                                 Context<KafkaProxy> context) {
        var clusters = ClustersUtil.distinctClusters(primary);
        var result = new HashMap<String, Service>(1 + (int) ((clusters.size() + 1) / 0.75f));
        for (var cluster : clusters) {
            result.put(cluster.getName(), clusterService(primary, cluster));
        }

        return result;
    }

    @Override
    public Map<String, Service> getSecondaryResources(
                                                      KafkaProxy primary,
                                                      Context<KafkaProxy> context) {
        Set<Service> secondaryResources = context.eventSourceRetriever().getResourceEventSourceFor(Service.class, "io.kroxylicious.kubernetes.operator.ClusterService")
                .getSecondaryResources(primary);
        return secondaryResources.stream()
                .filter(svc1 -> svc1.getMetadata().getName().contains("-cluster-"))
                .collect(Collectors.toMap(
                        svc -> {
                            String name = svc.getMetadata().getName();
                            return name.substring(0, name.indexOf("-cluster-"));
                        },
                        Function.identity()));
    }
}
