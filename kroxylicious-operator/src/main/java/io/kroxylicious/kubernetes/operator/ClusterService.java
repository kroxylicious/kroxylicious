/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.kubernetes.operator;

import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.processing.dependent.BulkDependentResource;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.CRUDKubernetesDependentResource;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.KubernetesDependent;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaproxyspec.Clusters;

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

    @Override
    public Map<String, Service> desiredResources(
                                                 KafkaProxy primary,
                                                 Context<KafkaProxy> context) {
        var clusters = ResourcesUtil.distinctClusters(primary);
        return clusters.stream()
                .filter(cluster -> !SharedKafkaProxyContext.isBroken(context, cluster))
                .flatMap(cluster -> servicesFor(cluster, primary))
                .collect(Collectors.toMap(
                        s -> s.getMetadata().getName(),
                        s -> s));
    }

    private Stream<Service> servicesFor(Clusters cluster, KafkaProxy primary) {
        ClusterSniExposition exposition = ClusterSniExposition.planExposition(primary, cluster);
        return exposition.getExpositions().stream().sorted().flatMap(host -> servicesFor(host, primary));
    }

    private Stream<Service> servicesFor(ClusterSniExposition.HostnameExposition exposition, KafkaProxy primary) {
        return exposition.services().stream().map(service -> {
            var serviceSpecBuilder = new ServiceBuilder()
                    .withNewMetadata()
                    .withName(service.name())
                    .withNamespace(primary.getMetadata().getNamespace())
                    .addToLabels(standardLabels(primary))
                    .addNewOwnerReferenceLike(ResourcesUtil.ownerReferenceTo(primary)).endOwnerReference()
                    .endMetadata()
                    .withNewSpec()
                    .withType(service.type())
                    .withSelector(ProxyDeployment.podLabels());
            Integer port = exposition.listener().getPort();
            serviceSpecBuilder.addNewPort()
                    .withName("tls")
                    .withPort(port)
                    .withTargetPort(new IntOrString(port))
                    .withProtocol("TCP")
                    .endPort();
            return serviceSpecBuilder.endSpec().build();
        });
    }

    @Override
    public Map<String, Service> getSecondaryResources(
                                                      KafkaProxy primary,
                                                      Context<KafkaProxy> context) {
        Set<Service> secondaryResources = context.eventSourceRetriever().getResourceEventSourceFor(Service.class)
                .getSecondaryResources(primary);
        return secondaryResources.stream()
                .collect(Collectors.toMap(
                        (s) -> s.getMetadata().getName(), // TODO what should this map to? Used to be that service name == vcluster name, no longer
                        Function.identity()));
    }
}
