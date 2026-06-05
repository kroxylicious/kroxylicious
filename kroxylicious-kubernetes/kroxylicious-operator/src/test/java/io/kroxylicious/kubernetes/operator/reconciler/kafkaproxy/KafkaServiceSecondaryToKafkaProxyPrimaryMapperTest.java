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
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaService;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class KafkaServiceSecondaryToKafkaProxyPrimaryMapperTest {

    @Test
    void kafkaServiceToProxyMapper() {
        // given
        KafkaProxy proxy = MapperTestSupport.buildProxy("proxy");
        EventSourceContext<KafkaProxy> eventSourceContext = MapperTestSupport.mockContextContaining(proxy);
        KafkaService kafkaService = MapperTestSupport.buildKafkaService("ref", 1L, 1L);
        SecondaryToPrimaryMapper<KafkaService> mapper = new KafkaServiceSecondaryToKafkaProxyPrimaryMapper(eventSourceContext);

        // when
        Set<ResourceID> primaryResourceIDs = mapper.toPrimaryResourceIDs(kafkaService);

        // then
        assertThat(primaryResourceIDs).containsExactly(ResourceID.fromResource(proxy));
    }

    @Test
    void kafkaServiceToProxyMapperIgnoresServiceWithStaleStatus() {
        // given
        KafkaProxy proxy = MapperTestSupport.buildProxy("proxy");
        EventSourceContext<KafkaProxy> eventSourceContext = MapperTestSupport.mockContextContaining(proxy);
        KafkaService kafkaService = MapperTestSupport.buildKafkaService("ref", 5L, 3L);
        SecondaryToPrimaryMapper<KafkaService> mapper = new KafkaServiceSecondaryToKafkaProxyPrimaryMapper(eventSourceContext);

        // when
        Set<ResourceID> primaryResourceIDs = mapper.toPrimaryResourceIDs(kafkaService);

        // then
        assertThat(primaryResourceIDs).isEmpty();
    }

    @Test
    void kafkaServiceToProxyMapperReturnsAllProxiesInNamespace() {
        // given — the mapper no longer filters by VKC reference, so all proxies in the namespace are returned
        KafkaProxy proxy1 = MapperTestSupport.buildProxy("proxy1");
        KafkaProxy proxy2 = MapperTestSupport.buildProxy("proxy2");
        EventSourceContext<KafkaProxy> eventSourceContext = MapperTestSupport.mockContextContaining(proxy1, proxy2);
        KafkaService kafkaService = MapperTestSupport.buildKafkaService("ref", 1L, 1L);
        SecondaryToPrimaryMapper<KafkaService> mapper = new KafkaServiceSecondaryToKafkaProxyPrimaryMapper(eventSourceContext);

        // when
        Set<ResourceID> primaryResourceIDs = mapper.toPrimaryResourceIDs(kafkaService);

        // then
        assertThat(primaryResourceIDs).containsExactlyInAnyOrder(ResourceID.fromResource(proxy1), ResourceID.fromResource(proxy2));
    }

    @Test
    void shouldReturnIdsWhenApiServerUnavailable() {
        // given
        KafkaProxy proxy = MapperTestSupport.buildProxy("proxy");
        EventSourceContext<KafkaProxy> context = mock();
        MapperTestSupport.stubFailingListOperationClient(context);
        MapperTestSupport.stubPrimaryCache(context, proxy);
        KafkaService kafkaService = MapperTestSupport.buildKafkaService("ref", 1L, 1L);
        SecondaryToPrimaryMapper<KafkaService> mapper = new KafkaServiceSecondaryToKafkaProxyPrimaryMapper(context);

        // when
        Set<ResourceID> primaryResourceIDs = mapper.toPrimaryResourceIDs(kafkaService);

        // then
        assertThat(primaryResourceIDs).containsExactly(ResourceID.fromResource(proxy));
    }
}
