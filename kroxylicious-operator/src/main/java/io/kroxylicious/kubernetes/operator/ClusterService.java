/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.kubernetes.operator;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.regex.Pattern;
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
@KubernetesDependent(genericFilter = ClusterFilter.class)
public class ClusterService
        extends CRUDKubernetesDependentResource<Service, KafkaProxy>
        implements BulkDependentResource<Service, KafkaProxy> {

    public ClusterService() {
        super(Service.class);
        useEventSourceWithName("clusters");
    }

    /**
     * @return The {@code metadata.name} of the desired {@code Service}.
     */
    static String serviceName(KafkaProxy primary, Clusters cluster) {
        Objects.requireNonNull(primary);
        Objects.requireNonNull(cluster);
        return cluster.getName() + "-cluster-" + primary.getMetadata().getName();
    }

    static Map<Integer, String> clusterPorts(KafkaProxy primary, Clusters cluster) {
        var clusters = primary.getSpec().getClusters();
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
                                     Clusters cluster,
                                     Context<KafkaProxy> context) {
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
        var clusters = primary.getSpec().getClusters();
        var result = new HashMap<String, Service>(1 + (int) ((clusters.size() + 1) / 0.75f));

        for (var cluster : clusters) {
            result.put(cluster.getName(), clusterService(primary, cluster, context));
        }

        return result;
    }

    @Override
    public Map<String, Service> getSecondaryResources(
                                                      KafkaProxy primary,
                                                      Context<KafkaProxy> context) {
        return context.getSecondaryResources(Service.class).stream()
                .collect(Collectors.toMap(
                        svc -> svc.getMetadata().getName(),
                        Function.identity()));
    }
}
