/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.reconciler.kafkaproxy;

import java.util.Set;

import org.junit.jupiter.api.Test;

import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import io.javaoperatorsdk.operator.processing.event.source.SecondaryToPrimaryMapper;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngress;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngressBuilder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class KafkaProxyIngressSecondaryToKafkaProxyPrimaryMapperTest {

    @Test
    void ingressToProxyMapper() {
        // given
        KafkaProxy proxy = MapperTestSupport.buildProxy("proxy");
        EventSourceContext<KafkaProxy> eventSourceContext = MapperTestSupport.mockContextContaining(proxy);
        SecondaryToPrimaryMapper<KafkaProxyIngress> mapper = new KafkaProxyIngressSecondaryToKafkaProxyPrimaryMapper(eventSourceContext);
        KafkaProxyIngress ingress = new KafkaProxyIngressBuilder().withNewMetadata().withName("ingress").endMetadata().withNewSpec().withNewProxyRef()
                .withName("proxy")
                .endProxyRef().endSpec().build();

        // when
        Set<ResourceID> primaryResourceIDs = mapper.toPrimaryResourceIDs(ingress);

        // then
        assertThat(primaryResourceIDs).containsExactly(ResourceID.fromResource(proxy));
    }

    @Test
    void ingressToProxyMapperIgnoresIngressWithStaleStatus() {
        // given
        KafkaProxy proxy = MapperTestSupport.buildProxy("proxy");
        EventSourceContext<KafkaProxy> eventSourceContext = MapperTestSupport.mockContextContaining(proxy);
        SecondaryToPrimaryMapper<KafkaProxyIngress> mapper = new KafkaProxyIngressSecondaryToKafkaProxyPrimaryMapper(eventSourceContext);
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

    @Test
    void shouldReturnIdsWhenApiServerUnavailable() {
        // given
        KafkaProxy proxy = MapperTestSupport.buildProxy("proxy");
        EventSourceContext<KafkaProxy> context = mock();
        MapperTestSupport.stubFailingListOperationClient(context);
        MapperTestSupport.stubPrimaryCache(context, proxy);
        SecondaryToPrimaryMapper<KafkaProxyIngress> mapper = new KafkaProxyIngressSecondaryToKafkaProxyPrimaryMapper(context);
        KafkaProxyIngress ingress = new KafkaProxyIngressBuilder().withNewMetadata().withName("ingress").endMetadata().withNewSpec().withNewProxyRef()
                .withName("proxy")
                .endProxyRef().endSpec().build();

        // when
        Set<ResourceID> primaryResourceIDs = mapper.toPrimaryResourceIDs(ingress);

        // then
        assertThat(primaryResourceIDs).containsExactly(ResourceID.fromResource(proxy));
    }
}
