/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.reconciler.virtualkafkacluster;

import java.util.Optional;
import java.util.Set;

import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.Service;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import io.javaoperatorsdk.operator.processing.event.source.SecondaryToPrimaryMapper;

import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.operator.ResourcesUtil;

class KubernetesServicesSecondaryToVirtualKafkaClusterPrimaryMapper implements SecondaryToPrimaryMapper<Service> {
    private final EventSourceContext<VirtualKafkaCluster> context;

    KubernetesServicesSecondaryToVirtualKafkaClusterPrimaryMapper(EventSourceContext<VirtualKafkaCluster> context) {
        this.context = context;
    }

    @Override
    public Set<ResourceID> toPrimaryResourceIDs(Service kubernetesService) {
        Optional<OwnerReference> clusterOwner = extractOwnerRefFromKubernetesService(kubernetesService,
                VirtualKafkaClusterReconciler.VIRTUAL_KAFKA_CLUSTER_KIND);
        if (clusterOwner.isPresent()) {
            return clusterOwner
                    .map(ownerRef -> new ResourceID(ownerRef.getName(), kubernetesService.getMetadata().getNamespace()))
                    .map(Set::of).orElse(Set.of());
        }
        Optional<OwnerReference> proxyOwner = extractOwnerRefFromKubernetesService(kubernetesService,
                VirtualKafkaClusterReconciler.KAFKA_PROXY_KIND);
        if (proxyOwner.isPresent()) {
            return ResourcesUtil.findReferrers(context, proxyOwner.get(), kubernetesService, VirtualKafkaCluster.class,
                    cluster -> Optional.of(cluster.getSpec().getProxyRef()));
        }
        else {
            return Set.of();
        }
    }

    private static Optional<OwnerReference> extractOwnerRefFromKubernetesService(Service service, String ownerKind) {
        return service.getMetadata()
                .getOwnerReferences()
                .stream()
                .filter(or -> ownerKind.equals(or.getKind()))
                .findFirst();
    }
}
