/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.reconciler.kafkaproxy;

import java.util.Set;
import java.util.stream.Stream;

import org.junit.jupiter.api.Named;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.processing.event.ResourceID;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * The single, shared secondary&rarr;primary mapper is type-agnostic: every secondary type maps to all
 * {@link KafkaProxy} primaries in its namespace, dropping only events from secondaries whose status is stale.
 */
class AllPrimariesInNamespaceMapperTest {

    static Stream<Arguments> freshSecondaries() {
        KafkaProxy proxy = MapperTestSupport.buildProxy("proxy");
        return Stream.of(
                Arguments.of(Named.of("filter", MapperTestSupport.buildFilter("filter", 1L, 1L))),
                Arguments.of(Named.of("ingress", MapperTestSupport.buildIngress("ingress", "proxy", 1L, 1L))),
                Arguments.of(Named.of("kafkaService", MapperTestSupport.buildKafkaService("svc", 1L, 1L))),
                Arguments.of(Named.of("virtualKafkaCluster", MapperTestSupport.baseVirtualKafkaClusterBuilder(proxy, "cluster", 1L, 1L).build())));
    }

    static Stream<Arguments> staleSecondaries() {
        KafkaProxy proxy = MapperTestSupport.buildProxy("proxy");
        return Stream.of(
                Arguments.of(Named.of("filter", MapperTestSupport.buildFilter("filter", 4L, 1L))),
                Arguments.of(Named.of("ingress", MapperTestSupport.buildIngress("ingress", "proxy", 23L, 20L))),
                Arguments.of(Named.of("kafkaService", MapperTestSupport.buildKafkaService("svc", 5L, 3L))),
                Arguments.of(Named.of("virtualKafkaCluster", MapperTestSupport.baseVirtualKafkaClusterBuilder(proxy, "cluster", 2L, 1L).build())));
    }

    @ParameterizedTest
    @MethodSource("freshSecondaries")
    void mapsFreshSecondaryToAllProxiesInNamespace(HasMetadata secondary) {
        // Given
        KafkaProxy proxy1 = MapperTestSupport.buildProxy("proxy1");
        KafkaProxy proxy2 = MapperTestSupport.buildProxy("proxy2");
        EventSourceContext<KafkaProxy> context = MapperTestSupport.mockContextContaining(proxy1, proxy2);
        AllPrimariesInNamespaceMapper mapper = new AllPrimariesInNamespaceMapper(context);

        // When
        Set<ResourceID> primaryResourceIDs = mapper.toPrimaryResourceIDs(secondary);

        // Then
        assertThat(primaryResourceIDs).containsExactlyInAnyOrder(ResourceID.fromResource(proxy1), ResourceID.fromResource(proxy2));
    }

    @ParameterizedTest
    @MethodSource("staleSecondaries")
    void ignoresSecondaryWithStaleStatus(HasMetadata secondary) {
        // Given
        KafkaProxy proxy = MapperTestSupport.buildProxy("proxy");
        EventSourceContext<KafkaProxy> context = MapperTestSupport.mockContextContaining(proxy);
        AllPrimariesInNamespaceMapper mapper = new AllPrimariesInNamespaceMapper(context);

        // When
        Set<ResourceID> primaryResourceIDs = mapper.toPrimaryResourceIDs(secondary);

        // Then
        assertThat(primaryResourceIDs).isEmpty();
    }

    @ParameterizedTest
    @MethodSource("freshSecondaries")
    void returnsIdsFromPrimaryCacheWhenApiServerListUnavailable(HasMetadata secondary) {
        // Given
        KafkaProxy proxy = MapperTestSupport.buildProxy("proxy");
        EventSourceContext<KafkaProxy> context = mock();
        MapperTestSupport.stubFailingListOperationClient(context);
        MapperTestSupport.stubPrimaryCache(context, proxy);
        AllPrimariesInNamespaceMapper mapper = new AllPrimariesInNamespaceMapper(context);

        // When
        Set<ResourceID> primaryResourceIDs = mapper.toPrimaryResourceIDs(secondary);

        // Then
        assertThat(primaryResourceIDs).containsExactly(ResourceID.fromResource(proxy));
    }
}
