/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.reconciler.virtualkafkacluster;

import java.util.Set;

import org.junit.jupiter.api.Test;

import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import io.javaoperatorsdk.operator.processing.event.source.SecondaryToPrimaryMapper;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaClusterBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.virtualkafkaclusterspec.IngressesBuilder;
import io.kroxylicious.kubernetes.operator.ResourcesUtil;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class KubernetesServicesSecondaryToVirtualKafkaClusterPrimaryMapperTest {

    @Test
    void kubernetesServicesSecondaryToPrimaryMapperServiceOwnedByProxy() {
        // given
        KafkaProxy proxy = new KafkaProxyBuilder().withNewMetadata().withName("proxy").endMetadata().build();
        VirtualKafkaCluster cluster = new VirtualKafkaClusterBuilder().withNewMetadata().withName("cluster").endMetadata().withNewSpec()
                .withIngresses(new IngressesBuilder()
                        .withNewIngressRef().withName("ingress").endIngressRef().build())
                .withNewProxyRef().withName("proxy").endProxyRef()
                .endSpec().build();
        EventSourceContext<VirtualKafkaCluster> eventSourceContext = MapperTestSupport.mockContextContaining(cluster);
        SecondaryToPrimaryMapper<Service> mapper = new KubernetesServicesSecondaryToVirtualKafkaClusterPrimary(eventSourceContext);
        OwnerReference clusterOwner = ResourcesUtil.newOwnerReferenceTo(proxy);
        Service service = new ServiceBuilder().withNewMetadata().withOwnerReferences(clusterOwner).endMetadata().build();

        // when
        Set<ResourceID> primaryResourceIDs = mapper.toPrimaryResourceIDs(service);

        // then
        assertThat(primaryResourceIDs).containsExactly(ResourceID.fromResource(cluster));
    }

    @Test
    void kubernetesServicesSecondaryToPrimaryMapperServiceOwnedByProxy_NoClustersRefProxy() {
        // given
        KafkaProxy proxy = new KafkaProxyBuilder().withNewMetadata().withName("proxy").endMetadata().build();
        EventSourceContext<VirtualKafkaCluster> eventSourceContext = MapperTestSupport.mockContextContaining();
        SecondaryToPrimaryMapper<Service> mapper = new KubernetesServicesSecondaryToVirtualKafkaClusterPrimary(eventSourceContext);
        OwnerReference clusterOwner = ResourcesUtil.newOwnerReferenceTo(proxy);
        Service service = new ServiceBuilder().withNewMetadata().withOwnerReferences(clusterOwner).endMetadata().build();

        // when
        Set<ResourceID> primaryResourceIDs = mapper.toPrimaryResourceIDs(service);

        // then
        assertThat(primaryResourceIDs).isEmpty();
    }

    @Test
    void kubernetesServicesSecondaryToPrimaryMapperServiceNotOwnedByProxyOrCluster() {
        // given
        EventSourceContext<VirtualKafkaCluster> eventSourceContext = mock();

        SecondaryToPrimaryMapper<Service> mapper = new KubernetesServicesSecondaryToVirtualKafkaClusterPrimary(eventSourceContext);
        Service service = new ServiceBuilder().withNewMetadata().withName("service").endMetadata().build();

        // when
        Set<ResourceID> primaryResourceIDs = mapper.toPrimaryResourceIDs(service);

        // then
        assertThat(primaryResourceIDs).isEmpty();
    }

    @Test
    void kubernetesServicesSecondaryToPrimaryMapperServiceOwnedByProxy_ManyClustersRefProxy() {
        // given
        KafkaProxy proxy = new KafkaProxyBuilder().withNewMetadata().withName("proxy").endMetadata().build();
        VirtualKafkaCluster cluster = new VirtualKafkaClusterBuilder().withNewMetadata().withName("cluster").endMetadata().withNewSpec()
                .withIngresses(new IngressesBuilder()
                        .withNewIngressRef().withName("ingress").endIngressRef().build())
                .withNewProxyRef().withName("proxy").endProxyRef()
                .endSpec().build();
        VirtualKafkaCluster cluster2 = new VirtualKafkaClusterBuilder().withNewMetadata().withName("cluster2").endMetadata().withNewSpec()
                .withIngresses(new IngressesBuilder()
                        .withNewIngressRef().withName("ingress").endIngressRef().build())
                .withNewProxyRef().withName("proxy").endProxyRef()
                .endSpec().build();
        EventSourceContext<VirtualKafkaCluster> eventSourceContext = MapperTestSupport.mockContextContaining(cluster, cluster2);
        SecondaryToPrimaryMapper<Service> mapper = new KubernetesServicesSecondaryToVirtualKafkaClusterPrimary(eventSourceContext);
        OwnerReference clusterOwner = ResourcesUtil.newOwnerReferenceTo(proxy);
        Service service = new ServiceBuilder().withNewMetadata().withOwnerReferences(clusterOwner).endMetadata().build();

        // when
        Set<ResourceID> primaryResourceIDs = mapper.toPrimaryResourceIDs(service);

        // then
        assertThat(primaryResourceIDs).containsExactlyInAnyOrder(ResourceID.fromResource(cluster), ResourceID.fromResource(cluster2));
    }

    @Test
    void kubernetesServicesSecondaryToPrimaryMapperServiceOwnedByCluster() {
        // given
        KafkaProxy proxy = new KafkaProxyBuilder().withNewMetadata().withName("proxy").endMetadata().build();
        // even though the Service is owned by a KafkaProxy that is referenced by this Cluster, it is irrelevant as
        // this cluster does not own the Service. This is here to check that this cluster is not included in the primary resource ids.
        VirtualKafkaCluster irrelevantCluster = new VirtualKafkaClusterBuilder().withNewMetadata().withName("cluster2").endMetadata().withNewSpec()
                .withIngresses(new IngressesBuilder()
                        .withNewIngressRef().withName("ingress").endIngressRef().build())
                .withNewProxyRef().withName("proxy").endProxyRef()
                .endSpec().build();

        VirtualKafkaCluster cluster = new VirtualKafkaClusterBuilder().withNewMetadata().withName("cluster").endMetadata().withNewSpec()
                .withIngresses(new IngressesBuilder()
                        .withNewIngressRef().withName("ingress").endIngressRef().build())
                .withNewProxyRef().withName("proxy").endProxyRef()
                .endSpec().build();
        EventSourceContext<VirtualKafkaCluster> eventSourceContext = MapperTestSupport.mockContextContaining(cluster, irrelevantCluster);
        SecondaryToPrimaryMapper<Service> mapper = new KubernetesServicesSecondaryToVirtualKafkaClusterPrimary(eventSourceContext);
        OwnerReference clusterOwner = ResourcesUtil.newOwnerReferenceTo(cluster);
        OwnerReference proxyOwner = ResourcesUtil.newOwnerReferenceTo(proxy);
        Service service = new ServiceBuilder().withNewMetadata().withOwnerReferences(clusterOwner, proxyOwner).endMetadata().build();

        // when
        Set<ResourceID> primaryResourceIDs = mapper.toPrimaryResourceIDs(service);

        // then
        assertThat(primaryResourceIDs).containsExactly(ResourceID.fromResource(cluster));
    }

}
