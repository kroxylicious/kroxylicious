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

import io.fabric8.kubernetes.api.model.Service;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.processing.dependent.BulkDependentResource;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.CRUDKubernetesDependentResource;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.KubernetesDependent;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngress;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.operator.ingress.IngressAllocator;
import io.kroxylicious.kubernetes.operator.ingress.ProxyIngressModel;

import static io.kroxylicious.kubernetes.operator.ResourcesUtil.name;
import static io.kroxylicious.kubernetes.operator.ResourcesUtil.toByNameMap;

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
        return name(cluster);
    }

    @Override
    public Map<String, Service> desiredResources(
                                                 KafkaProxy primary,
                                                 Context<KafkaProxy> context) {
        List<VirtualKafkaCluster> clusters = ResourcesUtil.clustersInNameOrder(context).toList();
        Set<KafkaProxyIngress> ingresses = context.getSecondaryResources(KafkaProxyIngress.class);
        ProxyIngressModel ingressModel = IngressAllocator.allocateProxyIngressModel(primary, clusters, ingresses);
        Stream<Service> serviceStream = clusters.stream()
                .filter(cluster -> !SharedKafkaProxyContext.isBroken(context, cluster))
                .flatMap(cluster -> ingressModel.clusterIngressModel(cluster).map(ProxyIngressModel.VirtualClusterIngressModel::services).orElse(Stream.empty()));
        return serviceStream.collect(toByNameMap());
    }

    @Override
    public Map<String, Service> getSecondaryResources(
                                                      KafkaProxy primary,
                                                      Context<KafkaProxy> context) {
        Set<Service> secondaryResources = context.eventSourceRetriever().getResourceEventSourceFor(Service.class)
                .getSecondaryResources(primary);
        return secondaryResources.stream().collect(toByNameMap());
    }
}
