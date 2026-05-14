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
import io.javaoperatorsdk.operator.processing.event.source.SecondaryToPrimaryMapper;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProtocolFilter;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProtocolFilterBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class KafkaProtocolFilterSecondaryToKafkaProxyPrimaryMapperTest {
    @Test
    void filterToProxyMapper() {
        EventSourceContext<KafkaProxy> eventSourceContext = mock();
        KubernetesClient client = mock();
        when(eventSourceContext.getClient()).thenReturn(client);
        MixedOperation<KafkaProxy, KubernetesResourceList<KafkaProxy>, Resource<KafkaProxy>> mockOperation = mock();
        when(client.resources(KafkaProxy.class)).thenReturn(mockOperation);
        KubernetesResourceList<KafkaProxy> mockList = mock();
        when(mockOperation.list()).thenReturn(mockList);
        when(mockOperation.inNamespace(any())).thenReturn(mockOperation);
        KafkaProxy proxy = MapperTestSupport.buildProxy("proxy");
        when(mockList.getItems()).thenReturn(List.of(proxy));
        SecondaryToPrimaryMapper<KafkaProtocolFilter> mapper = new KafkaProtocolFilterSecondaryToKafkaProxyPrimaryMapper(eventSourceContext);
        String namespace = "test";
        KafkaProtocolFilter filter = new KafkaProtocolFilterBuilder().withNewMetadata().withName("filter").withNamespace(namespace).endMetadata().build();
        Set<ResourceID> primaryResourceIDs = mapper.toPrimaryResourceIDs(filter);
        assertThat(primaryResourceIDs).containsExactly(ResourceID.fromResource(proxy));
        verify(mockOperation).inNamespace(namespace);
    }

    @Test
    void filterToProxyMapperIgnoresFilterWithStaleStatus() {
        // given
        EventSourceContext<KafkaProxy> eventSourceContext = mock();
        KubernetesClient client = mock();
        when(eventSourceContext.getClient()).thenReturn(client);
        MixedOperation<KafkaProxy, KubernetesResourceList<KafkaProxy>, Resource<KafkaProxy>> mockOperation = mock();
        when(client.resources(KafkaProxy.class)).thenReturn(mockOperation);
        KubernetesResourceList<KafkaProxy> mockList = mock();
        when(mockOperation.list()).thenReturn(mockList);
        when(mockOperation.inNamespace(any())).thenReturn(mockOperation);
        KafkaProxy proxy = MapperTestSupport.buildProxy("proxy");
        when(mockList.getItems()).thenReturn(List.of(proxy));
        SecondaryToPrimaryMapper<KafkaProtocolFilter> mapper = new KafkaProtocolFilterSecondaryToKafkaProxyPrimaryMapper(eventSourceContext);
        String namespace = "test";
        // @formatter:off
        KafkaProtocolFilter filter = new KafkaProtocolFilterBuilder()
                .withNewMetadata()
                .withGeneration(4L)
                .withName("filter")
                .withNamespace(namespace)
                .endMetadata()
                .withNewStatus()
                .withObservedGeneration(1L)
                .endStatus()
                .build();
        // @formatter:on
        // when
        Set<ResourceID> primaryResourceIDs = mapper.toPrimaryResourceIDs(filter);

        // then
        assertThat(primaryResourceIDs).isEmpty();
    }

}