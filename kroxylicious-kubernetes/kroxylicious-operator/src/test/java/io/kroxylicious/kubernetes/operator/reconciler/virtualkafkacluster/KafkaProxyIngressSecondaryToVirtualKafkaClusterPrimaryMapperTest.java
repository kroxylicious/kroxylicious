/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.reconciler.virtualkafkacluster;

import java.util.Set;

import org.junit.jupiter.api.Test;

import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import io.javaoperatorsdk.operator.processing.event.source.SecondaryToPrimaryMapper;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngress;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngressBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaClusterBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.virtualkafkaclusterspec.IngressesBuilder;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaProxyIngressSecondaryToVirtualKafkaClusterPrimaryMapperTest {

    @Test
    void ingressSecondaryToPrimaryMapper() {
        // given
        VirtualKafkaCluster cluster = new VirtualKafkaClusterBuilder().withNewMetadata().withName("cluster").endMetadata().withNewSpec()
                .withIngresses(new IngressesBuilder()
                        .withNewIngressRef().withName("ingress").endIngressRef().build())
                .endSpec().build();
        EventSourceContext<VirtualKafkaCluster> eventSourceContext = MapperTestSupport.mockContextContaining(cluster);
        SecondaryToPrimaryMapper<KafkaProxyIngress> mapper = new KafkaProxyIngressSecondaryToVirtualKafkaClusterPrimaryMapper(eventSourceContext);
        KafkaProxyIngress ingress = new KafkaProxyIngressBuilder().withNewMetadata().withName("ingress").endMetadata().withNewSpec().withNewProxyRef()
                .withName("proxy")
                .endProxyRef().endSpec().build();

        // when
        Set<ResourceID> primaryResourceIDs = mapper.toPrimaryResourceIDs(ingress);

        // then
        assertThat(primaryResourceIDs).containsExactly(ResourceID.fromResource(cluster));
    }

    @Test
    void ingressSecondaryToPrimaryMapperIgnoresIngressWithStaleStatus() {
        // given
        VirtualKafkaCluster cluster = new VirtualKafkaClusterBuilder().withNewMetadata().withName("cluster").endMetadata().withNewSpec()
                .withIngresses(new IngressesBuilder().withNewIngressRef().withName("ingress").endIngressRef().build()).endSpec().build();
        EventSourceContext<VirtualKafkaCluster> eventSourceContext = MapperTestSupport.mockContextContaining(cluster);
        SecondaryToPrimaryMapper<KafkaProxyIngress> mapper = new KafkaProxyIngressSecondaryToVirtualKafkaClusterPrimaryMapper(eventSourceContext);
        // @formatter:off
        KafkaProxyIngress ingress = new KafkaProxyIngressBuilder()
                .withNewMetadata()
                .withName("ingress")
                .withGeneration(23L)
                .endMetadata()
                .withNewSpec()
                .withNewProxyRef()
                .withName("proxy")
                .endProxyRef()
                .endSpec()
                .withNewStatus()
                .withObservedGeneration(20L)
                .endStatus()
                .build();
        // @formatter:on

        // when
        Set<ResourceID> primaryResourceIDs = mapper.toPrimaryResourceIDs(ingress);

        // then
        assertThat(primaryResourceIDs).isEmpty();
    }
}
