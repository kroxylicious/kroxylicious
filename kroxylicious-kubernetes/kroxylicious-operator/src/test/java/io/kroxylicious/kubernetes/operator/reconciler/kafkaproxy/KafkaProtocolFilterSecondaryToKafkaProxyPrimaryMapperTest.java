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

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProtocolFilter;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProtocolFilterBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class KafkaProtocolFilterSecondaryToKafkaProxyPrimaryMapperTest {

    @Test
    void filterToProxyMapper() {
        // given
        KafkaProxy proxy = MapperTestSupport.buildProxy("proxy");
        EventSourceContext<KafkaProxy> eventSourceContext = MapperTestSupport.mockContextContaining(proxy);
        SecondaryToPrimaryMapper<KafkaProtocolFilter> mapper = new KafkaProtocolFilterSecondaryToKafkaProxyPrimaryMapper(eventSourceContext);
        KafkaProtocolFilter filter = new KafkaProtocolFilterBuilder().withNewMetadata().withName("filter").withNamespace("test").endMetadata().build();

        // when
        Set<ResourceID> primaryResourceIDs = mapper.toPrimaryResourceIDs(filter);

        // then
        assertThat(primaryResourceIDs).containsExactly(ResourceID.fromResource(proxy));
    }

    @Test
    void filterToProxyMapperIgnoresFilterWithStaleStatus() {
        // given
        KafkaProxy proxy = MapperTestSupport.buildProxy("proxy");
        EventSourceContext<KafkaProxy> eventSourceContext = MapperTestSupport.mockContextContaining(proxy);
        SecondaryToPrimaryMapper<KafkaProtocolFilter> mapper = new KafkaProtocolFilterSecondaryToKafkaProxyPrimaryMapper(eventSourceContext);
        // @formatter:off
        KafkaProtocolFilter filter = new KafkaProtocolFilterBuilder()
                .withNewMetadata()
                .withGeneration(4L)
                .withName("filter")
                .withNamespace("test")
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

    @Test
    void shouldReturnIdsWhenApiServerUnavailable() {
        // given
        KafkaProxy proxy = MapperTestSupport.buildProxy("proxy");
        EventSourceContext<KafkaProxy> context = mock();
        MapperTestSupport.stubFailingListOperationClient(context);
        MapperTestSupport.stubPrimaryCache(context, proxy);
        SecondaryToPrimaryMapper<KafkaProtocolFilter> mapper = new KafkaProtocolFilterSecondaryToKafkaProxyPrimaryMapper(context);
        KafkaProtocolFilter filter = new KafkaProtocolFilterBuilder().withNewMetadata().withName("filter").withNamespace("test").endMetadata().build();

        // when
        Set<ResourceID> primaryResourceIDs = mapper.toPrimaryResourceIDs(filter);

        // then
        assertThat(primaryResourceIDs).containsExactly(ResourceID.fromResource(proxy));
    }
}
