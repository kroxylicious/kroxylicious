/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.reconciler.kafkaproxy;

import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;

import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import io.javaoperatorsdk.operator.processing.event.source.PrimaryToSecondaryMapper;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngress;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngressBuilder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class KafkaProxyPrimaryToKafkaProxyIngressSecondaryMapperTest {

    @Test
    void proxyToIngressMapper() {
        EventSourceContext<KafkaProxy> eventSourceContext = mock();
        KubernetesClient client = mock();
        when(eventSourceContext.getClient()).thenReturn(client);
        MixedOperation<KafkaProxyIngress, KubernetesResourceList<KafkaProxyIngress>, Resource<KafkaProxyIngress>> mockOperation = mock();
        when(client.resources(KafkaProxyIngress.class)).thenReturn(mockOperation);
        KubernetesResourceList<KafkaProxyIngress> mockList = mock();
        when(mockOperation.list()).thenReturn(mockList);
        when(mockOperation.inNamespace(any())).thenReturn(mockOperation);
        KafkaProxyIngress ingress = new KafkaProxyIngressBuilder().withNewMetadata().withName("ingress").endMetadata().withNewSpec().withNewProxyRef()
                .withName("proxy")
                .endProxyRef().endSpec().build();
        KafkaProxyIngress ingressForAnotherProxy = new KafkaProxyIngressBuilder().withNewMetadata().withName("ingress2").endMetadata().withNewSpec().withNewProxyRef()
                .withName("anotherProxy")
                .endProxyRef().endSpec().build();
        when(mockList.getItems()).thenReturn(List.of(ingress, ingressForAnotherProxy));
        PrimaryToSecondaryMapper<KafkaProxy> mapper = new KafkaProxyPrimaryToKafkaProxyIngressSecondaryMapper(eventSourceContext);
        KafkaProxy proxy = new KafkaProxyBuilder().withNewMetadata().withName("proxy").endMetadata().build();
        Set<ResourceID> secondaryResourceIDs = mapper.toSecondaryResourceIDs(proxy);
        assertThat(secondaryResourceIDs).containsExactly(ResourceID.fromResource(ingress));
    }

}