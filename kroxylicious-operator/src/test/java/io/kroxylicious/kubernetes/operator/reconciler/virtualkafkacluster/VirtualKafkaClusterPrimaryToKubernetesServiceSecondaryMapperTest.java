/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.reconciler.virtualkafkacluster;

import java.util.Set;

import org.junit.jupiter.api.Test;

import io.javaoperatorsdk.operator.processing.event.ResourceID;

import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaClusterBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.virtualkafkaclusterspec.IngressesBuilder;

import static org.assertj.core.api.Assertions.assertThat;

class VirtualKafkaClusterPrimaryToKubernetesServiceSecondaryMapperTest {

    @Test
    void kubernetesServicesPrimaryToSecondaryMapper() {
        // given
        VirtualKafkaCluster cluster = new VirtualKafkaClusterBuilder().withNewMetadata()
                .withName("cluster").withNamespace("namespace").endMetadata().withNewSpec()
                .withIngresses(new IngressesBuilder()
                        .withNewIngressRef().withName("ingress").endIngressRef().build())
                .withNewProxyRef().withName("proxy").endProxyRef()
                .endSpec().build();

        // when
        Set<ResourceID> secondaryResourceIDs = new VirtualKafkaClusterPrimaryToKubernetesServicesSecondaryMapper().toSecondaryResourceIDs(cluster);

        // then
        ResourceID clusterIpBootstrapServiceId = new ResourceID("cluster-ingress-bootstrap", "namespace");
        ResourceID proxyLoadbalancerServiceId = new ResourceID("proxy-sni", "namespace");
        assertThat(secondaryResourceIDs).containsExactlyInAnyOrder(clusterIpBootstrapServiceId, proxyLoadbalancerServiceId);
    }

    @Test
    void kubernetesServicesPrimaryToSecondaryMapperMultipleIngresses() {
        // given
        String clusterName = "cluster";
        String ingressName = "ingress";
        String ingressName2 = "ingress2";
        String proxyName = "proxy";
        String namespace = "namespace";
        VirtualKafkaCluster cluster = new VirtualKafkaClusterBuilder().withNewMetadata()
                .withName(clusterName).withNamespace(namespace).endMetadata().withNewSpec()
                .withIngresses(new IngressesBuilder()
                        .withNewIngressRef().withName(ingressName).endIngressRef().build(),
                        new IngressesBuilder()
                                .withNewIngressRef().withName(ingressName2).endIngressRef().build())
                .withNewProxyRef().withName(proxyName).endProxyRef()
                .endSpec().build();

        // when
        Set<ResourceID> secondaryResourceIDs = new VirtualKafkaClusterPrimaryToKubernetesServicesSecondaryMapper().toSecondaryResourceIDs(cluster);

        // then
        ResourceID clusterIpBootstrapServiceId = new ResourceID(clusterName + "-" + ingressName + "-bootstrap", namespace);
        ResourceID clusterIpBootstrapServiceId2 = new ResourceID(clusterName + "-" + ingressName2 + "-bootstrap", namespace);
        ResourceID proxyLoadbalancerServiceId = new ResourceID(proxyName + "-sni", namespace);
        assertThat(secondaryResourceIDs).containsExactlyInAnyOrder(clusterIpBootstrapServiceId, clusterIpBootstrapServiceId2, proxyLoadbalancerServiceId);
    }
}
