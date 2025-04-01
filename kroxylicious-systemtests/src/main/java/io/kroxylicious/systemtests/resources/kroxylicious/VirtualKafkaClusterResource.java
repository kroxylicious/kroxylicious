/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.resources.kroxylicious;

import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;

import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.systemtests.Constants;
import io.kroxylicious.systemtests.resources.ResourceType;

import static io.kroxylicious.systemtests.k8s.KubeClusterResource.kubeClient;

public class VirtualKafkaClusterResource implements ResourceType<VirtualKafkaCluster> {

    @Override
    public String getKind() {
        return Constants.VIRTUAL_KAFKA_CLUSTER_KIND;
    }

    @Override
    public VirtualKafkaCluster get(String namespace, String name) {
        return virtualKafkaClusterClient().inNamespace(namespace).withName(name).get();
    }

    @Override
    public void create(VirtualKafkaCluster resource) {
        virtualKafkaClusterClient().inNamespace(resource.getMetadata().getNamespace()).resource(resource).create();
    }

    @Override
    public void delete(VirtualKafkaCluster resource) {
        virtualKafkaClusterClient().inNamespace(resource.getMetadata().getNamespace()).withName(
                resource.getMetadata().getName()).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
    }

    @Override
    public void update(VirtualKafkaCluster resource) {
        virtualKafkaClusterClient().inNamespace(resource.getMetadata().getNamespace()).resource(resource).update();
    }

    @Override
    public boolean waitForReadiness(VirtualKafkaCluster resource) {
        return resource != null;
    }

    /**
     * Kafka Proxy mixed operation.
     *
     * @return the mixed operation
     */
    public static MixedOperation<VirtualKafkaCluster, KubernetesResourceList<VirtualKafkaCluster>, Resource<VirtualKafkaCluster>> virtualKafkaClusterClient() {
        return kubeClient().getClient().resources(VirtualKafkaCluster.class);
    }
}
