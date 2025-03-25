/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.javaoperatorsdk.operator.api.reconciler.Context;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngress;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngressBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaService;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaServiceBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaClusterBuilder;
import io.kroxylicious.kubernetes.filter.api.v1alpha1.KafkaProtocolFilter;
import io.kroxylicious.kubernetes.filter.api.v1alpha1.KafkaProtocolFilterBuilder;
import io.kroxylicious.kubernetes.operator.assertj.ConditionAssert;
import io.kroxylicious.kubernetes.operator.assertj.VirtualKafkaClusterStatusAssert;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class VirtualKafkaClusterReconcilerTest {

    // @formatter:off
    public static final VirtualKafkaCluster CLUSTER_NO_FILTERS = new VirtualKafkaClusterBuilder()
            .withNewMetadata()
                .withName("foo")
                .withGeneration(42L)
            .endMetadata()
            .withNewSpec()
                .withNewProxyRef()
                    .withName("my-proxy")
                .endProxyRef()
                .addNewIngressRef()
                    .withName("my-ingress")
                .endIngressRef()
                .withNewTargetKafkaServiceRef()
                    .withName("my-kafka")
                .endTargetKafkaServiceRef()
            .endSpec()
            .build();
    public static final VirtualKafkaCluster CLUSTER_ONE_FILTER = new VirtualKafkaClusterBuilder(CLUSTER_NO_FILTERS)
            .editSpec()
                .addNewFilterRef()
                    .withName("my-filter")
                .endFilterRef()
            .endSpec()
            .build();

    public static final KafkaProxy PROXY = new KafkaProxyBuilder()
            .withNewMetadata()
                .withName("my-proxy")
                .withGeneration(101L)
            .endMetadata()
            .withNewSpec()
            .endSpec()
            .build();

    public static final KafkaService SERVICE = new KafkaServiceBuilder()
            .withNewMetadata()
                .withName("my-kafka")
                .withGeneration(201L)
            .endMetadata()
            .withNewSpec()
            .endSpec()
            .build();

    public static final KafkaProxyIngress INGRESS = new KafkaProxyIngressBuilder()
            .withNewMetadata()
                .withName("my-ingress")
                .withGeneration(301L)
            .endMetadata()
            .withNewSpec()
            .endSpec()
            .build();

    public static final KafkaProtocolFilter FILTER_MY_FILTER = new KafkaProtocolFilterBuilder()
            .withNewMetadata()
            .withName("my-filter")
            .withGeneration(401L)
            .endMetadata()
            .withNewSpec()
            .endSpec()
            .build();
    // @formatter:on

    static List<Arguments> shouldSetResolvedRefsToTrueOrFalse() {
        return List.of(
                Arguments.argumentSet("no filter",
                        CLUSTER_NO_FILTERS,
                        Optional.of(PROXY),
                        Optional.of(SERVICE),
                        Stream.of(INGRESS),
                        Stream.of(),
                        (Consumer<ConditionAssert>) ConditionAssert::isResolvedRefsTrue),
                Arguments.argumentSet("one filter",
                        CLUSTER_ONE_FILTER,
                        Optional.of(PROXY),
                        Optional.of(SERVICE),
                        Stream.of(INGRESS),
                        Stream.of(FILTER_MY_FILTER),
                        (Consumer<ConditionAssert>) ConditionAssert::isResolvedRefsTrue),
                Arguments.argumentSet("proxy not found",
                        CLUSTER_NO_FILTERS,
                        Optional.empty(),
                        Optional.of(SERVICE),
                        Stream.of(INGRESS),
                        Stream.of(),
                        (Consumer<ConditionAssert>) ca -> ca.isResolvedRefsFalse(
                                "ReferencedResourcesNotFound",
                                "spec.proxyRef references kafkaproxy.kroxylicious.io/my-proxy")),
                Arguments.argumentSet("service not found",
                        CLUSTER_NO_FILTERS,
                        Optional.of(PROXY),
                        Optional.empty(),
                        Stream.of(INGRESS),
                        Stream.of(),
                        (Consumer<ConditionAssert>) ca -> ca.isResolvedRefsFalse(
                                "ReferencedResourcesNotFound",
                                "spec.targetKafkaServiceRef references kafkaservice.kroxylicious.io/my-kafka")),
                Arguments.argumentSet("ingress not found",
                        CLUSTER_NO_FILTERS,
                        Optional.of(PROXY),
                        Optional.of(SERVICE),
                        Stream.of(),
                        Stream.of(),
                        (Consumer<ConditionAssert>) ca -> ca.isResolvedRefsFalse(
                                "ReferencedResourcesNotFound",
                                "spec.ingressRefs references kafkaproxyingress.kroxylicious.io/my-ingress")),
                Arguments.argumentSet("filter not found",
                        CLUSTER_ONE_FILTER,
                        Optional.of(PROXY),
                        Optional.of(SERVICE),
                        Stream.of(INGRESS),
                        Stream.of(),
                        (Consumer<ConditionAssert>) ca -> ca.isResolvedRefsFalse(
                                "ReferencedResourcesNotFound",
                                "spec.filterRefs references kafkaprotocolfilter.filter.kroxylicious.io/my-filter")));
    }

    @ParameterizedTest
    @MethodSource
    void shouldSetResolvedRefsToTrueOrFalse(VirtualKafkaCluster cluster,
                                            Optional<KafkaProxy> existingProxy,
                                            Optional<KafkaService> existingService,
                                            Stream<KafkaProxyIngress> existingIngresses,
                                            Stream<KafkaProtocolFilter> existingFilters,
                                            Consumer<ConditionAssert> asserter) {
        // given
        Clock z = Clock.fixed(Instant.EPOCH, ZoneId.of("Z"));
        var reconciler = new VirtualKafkaClusterReconciler(z);

        Context<VirtualKafkaCluster> context = mock(Context.class);
        when(context.getSecondaryResource(KafkaProxy.class, VirtualKafkaClusterReconciler.PROXY_EVENT_SOURCE_NAME)).thenReturn(existingProxy);
        when(context.getSecondaryResource(KafkaService.class, VirtualKafkaClusterReconciler.SERVICES_EVENT_SOURCE_NAME)).thenReturn(existingService);
        when(context.getSecondaryResourcesAsStream(KafkaProxyIngress.class)).thenReturn(existingIngresses);
        when(context.getSecondaryResourcesAsStream(KafkaProtocolFilter.class)).thenReturn(existingFilters);

        // when
        var update = reconciler.reconcile(cluster, context);

        // then
        assertThat(update).isNotNull();
        assertThat(update.isPatchStatus()).isTrue();
        assertThat(update.getResource()).isPresent();
        ConditionAssert conditionAssert = VirtualKafkaClusterStatusAssert.assertThat(update.getResource().get().getStatus())
                .hasObservedGenerationInSyncWithMetadataOf(cluster)
                .singleCondition()
                .hasObservedGenerationInSyncWithMetadataOf(cluster)
                .hasLastTransitionTime(ZonedDateTime.ofInstant(z.instant(), z.getZone()));
        asserter.accept(conditionAssert);

    }

    @Test
    void shouldSetResolvedRefsToUnknown() {
        // given
        Clock z = Clock.fixed(Instant.EPOCH, ZoneId.of("Z"));
        var reconciler = new VirtualKafkaClusterReconciler(z);

        Context<VirtualKafkaCluster> context = mock(Context.class);

        // when
        var update = reconciler.updateErrorStatus(CLUSTER_NO_FILTERS, context, new RuntimeException("Boom!"));

        // then
        assertThat(update).isNotNull();
        assertThat(update.getResource()).isPresent();
        VirtualKafkaClusterStatusAssert.assertThat(update.getResource().get().getStatus())
                .hasObservedGenerationInSyncWithMetadataOf(CLUSTER_NO_FILTERS)
                .singleCondition()
                .hasObservedGenerationInSyncWithMetadataOf(CLUSTER_NO_FILTERS)
                .isResolvedRefsUnknown("java.lang.RuntimeException", "Boom!")
                .hasLastTransitionTime(ZonedDateTime.ofInstant(z.instant(), z.getZone()));

    }
}
